import io
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

import agent_bus_cli
from adapter_check import run_adapter_check, _stop_group


REPO = Path(__file__).resolve().parents[1]


class GroupCleanupTests(unittest.TestCase):
    def test_darwin_zombie_or_absent_group_preserves_cleanup_result(self):
        for denied_signal in (signal.SIGTERM, signal.SIGKILL):
            for listing in ('123 Z\n456 S\n', '456 S\n'):
                with self.subTest(signal=denied_signal, listing=listing):
                    process = Mock(pid=123)
                    process.poll.return_value = -15
                    signals = ([PermissionError()] if denied_signal == signal.SIGTERM
                               else [None, PermissionError()])
                    with patch('adapter_check.sys.platform', 'darwin'), \
                            patch('adapter_check.os.killpg', side_effect=signals) as kill, \
                            patch('adapter_check.subprocess.run', return_value=
                                  subprocess.CompletedProcess([], 0, listing, '')) as inspect:
                        _stop_group(process)
                    self.assertEqual(denied_signal, kill.call_args.args[1])
                    self.assertEqual(1.0, inspect.call_args.kwargs['timeout'])
                    process.wait.assert_called()

    def test_permission_denial_does_not_hide_live_or_unverifiable_members(self):
        cases = [
            subprocess.CompletedProcess([], 0, '123 S\n', ''),
            subprocess.CompletedProcess([], 0, '123 Z\n123 R\n', ''),
            subprocess.CompletedProcess([], 0, 'malformed\n', ''),
            subprocess.CompletedProcess([], 0, '', ''),
            subprocess.CompletedProcess([], 1, '456 S\n', 'denied'),
            subprocess.TimeoutExpired('ps', 1), OSError('ps unavailable'),
        ]
        for result in cases:
            with self.subTest(result=result):
                process = Mock(pid=123)
                process.poll.return_value = 0
                kwargs = {'side_effect': result} if isinstance(result, Exception) else {'return_value': result}
                with patch('adapter_check.sys.platform', 'darwin'), \
                        patch('adapter_check.os.killpg', side_effect=PermissionError()), \
                        patch('adapter_check.subprocess.run', **kwargs):
                    with self.assertRaisesRegex(PermissionError, 'child processes may remain'):
                        _stop_group(process)

    def test_live_leader_and_non_darwin_denials_are_not_suppressed(self):
        for platform, returncode in [('darwin', None), ('linux', 0)]:
            with self.subTest(platform=platform):
                process = Mock(pid=123)
                process.poll.return_value = returncode
                with patch('adapter_check.sys.platform', platform), \
                        patch('adapter_check.os.killpg', side_effect=PermissionError()), \
                        patch('adapter_check.subprocess.run') as inspect:
                    with self.assertRaises(PermissionError):
                        _stop_group(process)
                    inspect.assert_not_called()


class AdapterCheckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.original_directory = Path.cwd()
        os.chdir(self.root)

    def tearDown(self):
        os.chdir(self.original_directory)
        self.temp.cleanup()

    def invoke(self, source, *, timeout='1.5', json_output=True):
        (self.root / 'trial_adapter.py').write_text(source, encoding='utf-8')
        out, err = io.StringIO(), io.StringIO()
        args = ['adapter', 'check', '--python-target', 'trial_adapter:Agent', '--timeout', timeout]
        if json_output:
            args.append('--json')
        start = time.monotonic()
        code = agent_bus_cli.main(args, stdout=out, stderr=err)
        self.assertLess(time.monotonic() - start, 8, 'probe did not return within bounded cleanup')
        return code, out.getvalue(), err.getvalue()

    def test_normal_probe_keeps_stdout_json_clean(self):
        code, out, err = self.invoke(
            "import sys\nfrom agent_bus import Completed\n"
            "print('import chatter')\n"
            "class Agent:\n"
            " def execute(self, assignment):\n"
            "  print('x'*100000)\n"
            "  print('stderr chatter', file=sys.stderr)\n"
            "  return Completed('done', {'value':42})\n", timeout='10')
        self.assertEqual(0, code, err)
        report = json.loads(out)
        self.assertTrue(report['ok'])
        self.assertEqual('Completed', report['outcome_type'])
        self.assertNotIn('chatter', out)
        self.assertEqual('', err)

    def test_import_constructor_execute_close_and_exit_hooks_are_bounded(self):
        sources = {
            'import': "import time\ntime.sleep(60)\nclass Agent: pass\n",
            'constructor': "import time\nclass Agent:\n def __init__(self): time.sleep(60)\n",
            'execute': "import time\nclass Agent:\n def execute(self,a): time.sleep(60)\n",
            'ignores-term': "import signal,time\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\nclass Agent:\n def execute(self,a): time.sleep(60)\n",
            'close': "import time\nfrom agent_bus import Completed\nclass Agent:\n def execute(self,a): return Completed('done')\n def close(self): time.sleep(60)\n",
            'exit': "import time,atexit\nfrom agent_bus import Completed\natexit.register(lambda: time.sleep(60))\nclass Agent:\n def execute(self,a): return Completed('done')\n",
        }
        for phase, source in sources.items():
            with self.subTest(phase=phase):
                code, out, err = self.invoke(source)
                self.assertEqual(4, code, err)
                report = json.loads(out)
                self.assertFalse(report['ok'])
                self.assertEqual('probe timeout', report['checks'][0]['name'])
                self.assertIn('--timeout', report['checks'][0]['detail'])

    def test_load_errors_are_actionable_and_keep_exit_two(self):
        code, out, err = self.invoke('raise ImportError("missing dependency")\n')
        self.assertEqual(2, code)
        self.assertEqual('', out)
        self.assertIn('not importable', err)

    def test_blocking_config_read_is_inside_timeout(self):
        pipe = self.root / 'adapter-config.fifo'
        os.mkfifo(pipe)
        start = time.monotonic()
        report = run_adapter_check(integration_config=str(pipe), timeout=1.5)
        self.assertLess(time.monotonic() - start, 8)
        self.assertFalse(report.ok)
        self.assertEqual('probe timeout', report.checks[0].name)

    def test_probe_exception_is_a_failed_report(self):
        code, out, err = self.invoke("class Agent:\n def execute(self,a): raise RuntimeError('probe broke')\n")
        self.assertEqual(4, code, err)
        self.assertIn('probe broke', out)

    def test_abrupt_exit_is_not_reported_as_success(self):
        for exit_code in (0, 7):
            with self.subTest(exit_code=exit_code):
                code, out, err = self.invoke(f'import os\nos._exit({exit_code})\n')
                self.assertEqual(4, code, err)
                self.assertFalse(json.loads(out)['ok'])

    def test_large_report_is_bounded(self):
        code, out, err = self.invoke("class Agent:\n def execute(self,a): raise RuntimeError('x'*100000)\n")
        self.assertEqual(4, code, err)
        self.assertLess(len(out), 2048)
        self.assertIn('64 KiB', out)

    def test_nonregular_report_cannot_hang_the_parent(self):
        code, out, err = self.invoke(
            "import os,sys\npath=sys.argv[sys.argv.index('--report')+1]\n"
            "os.mkfifo(path)\nos._exit(0)\n")
        self.assertEqual(4, code, err)
        self.assertEqual('probe report', json.loads(out)['checks'][0]['name'])

    def assert_not_running(self, pid):
        # An orphan may briefly remain a zombie pending OS reaping; it is no
        # longer executing. Never signal an arbitrary PID during assertions.
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            result = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)],
                                    capture_output=True, text=True, timeout=2)
            if not result.stdout.strip() or result.stdout.strip().startswith('Z'):
                return
            time.sleep(0.05)
        self.fail(f'trial child {pid} is still running')

    def child_source(self, *, hang):
        child = ("import os,signal,time; from pathlib import Path; "
                 "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                 "Path('descendant.pid').write_text(str(os.getpid())); time.sleep(60)")
        return (
            "import subprocess,sys,time\nfrom pathlib import Path\nfrom agent_bus import Completed\n"
            "class Agent:\n def execute(self,a):\n"
            f"  subprocess.Popen([sys.executable,'-c',{child!r}])\n"
            "  while not Path('descendant.pid').exists(): time.sleep(.01)\n"
            + ("  time.sleep(60)\n" if hang else "  return Completed('done')\n"))

    def test_same_group_descendant_is_stopped_on_success_and_timeout(self):
        for hang in (False, True):
            with self.subTest(hang=hang):
                path = self.root / 'descendant.pid'
                path.unlink(missing_ok=True)
                code, out, err = self.invoke(self.child_source(hang=hang))
                self.assertEqual(4 if hang else 0, code, err)
                self.assertTrue(path.exists(), out)
                self.assert_not_running(int(path.read_text()))

    def test_cli_interrupt_stops_check_group(self):
        (self.root / 'trial_adapter.py').write_text(self.child_source(hang=True))
        for interruption in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=interruption):
                path = self.root / 'descendant.pid'
                path.unlink(missing_ok=True)
                proc = subprocess.Popen(
                    [sys.executable, str(REPO / 'agent_bus_cli.py'), 'adapter', 'check',
                     '--python-target', 'trial_adapter:Agent', '--timeout', '20'],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
                try:
                    deadline = time.monotonic() + 10
                    while not path.exists() and time.monotonic() < deadline:
                        self.assertIsNone(proc.poll())
                        time.sleep(0.05)
                    self.assertTrue(path.exists())
                    proc.send_signal(interruption)
                    out, err = proc.communicate(timeout=5)
                    self.assertEqual(130, proc.returncode, (out, err))
                    self.assert_not_running(int(path.read_text()))
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait()

    def test_configured_cli_timeout_owns_its_child(self):
        script = self.root / 'cli_agent.py'
        script.write_text("import os,time\nfrom pathlib import Path\nPath('cli.pid').write_text(str(os.getpid()))\ntime.sleep(60)\n")
        config = self.root / 'adapter.json'
        config.write_text(json.dumps({'schema_version': 1, 'worker': {'name':'trial'},
            'adapter': {'type':'cli', 'command':[sys.executable,str(script)],
                        'protocol_version':1, 'timeout_seconds':60, 'working_directory':str(self.root)}}))
        report = run_adapter_check(integration_config=str(config), timeout=1.5)
        self.assertFalse(report.ok)
        self.assertEqual('probe timeout', report.checks[0].name)
        self.assert_not_running(int((self.root / 'cli.pid').read_text()))

    def test_configured_cli_success_keeps_versioned_protocol(self):
        script = self.root / 'cli_agent.py'
        script.write_text(
            "import json,sys\nrequest=json.load(sys.stdin)\n"
            "assert request['protocol']['version']==1\n"
            "print(json.dumps({'protocol':{'name':'agent-bus.executor','version':1},"
            "'outcome':{'status':'completed','summary':'ok','result':{}}}))\n")
        config = self.root / 'adapter.json'
        config.write_text(json.dumps({'schema_version':1,'worker':{'name':'trial'},
            'adapter':{'type':'cli','command':[sys.executable,str(script)],
                       'protocol_version':1,'working_directory':str(self.root)}}))
        report = run_adapter_check(integration_config=str(config), timeout=10)
        self.assertTrue(report.ok, report)
        self.assertEqual('Completed', report.outcome_type)

    def test_configured_http_success_uses_real_loopback_endpoint(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                requests.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                body = json.dumps({'protocol':{'name':'agent-bus.executor','version':1},
                                   'outcome':{'status':'completed','summary':'ok','result':{}}}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            config = self.root / 'http.json'
            config.write_text(json.dumps({'schema_version':1,'worker':{'name':'trial'},
                'adapter':{'type':'http','endpoint':f'http://127.0.0.1:{server.server_port}/run',
                           'protocol_version':1}}))
            report = run_adapter_check(integration_config=str(config), timeout=10)
            self.assertTrue(report.ok, report)
            self.assertEqual(1, len(requests))
            self.assertTrue(requests[0]['assignment']['context']['agent_bus_conformance_probe'])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_timeout_validation_rejects_nonpositive_and_nonfinite_values(self):
        for value in ('0','-1','nan','inf'):
            with self.subTest(value=value), self.assertRaises(SystemExit) as error:
                agent_bus_cli.build_parser().parse_args(
                    ['adapter','check','--python-target','x:Agent','--timeout',value])
            self.assertEqual(2, error.exception.code)


if __name__ == '__main__':
    unittest.main()
