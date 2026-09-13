"""Verify an installed wheel, never an editable checkout, using a local demo.

Only the standard library is needed by the runner. Evidence is retained; the
fresh venv and runtime directory are disposable and cleaned up on exit.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import venv


IMPORT_PROBE = """
import importlib, importlib.metadata, json, pathlib, sys
prefix = pathlib.Path(sys.prefix).resolve()
modules = ['agent_bus', 'agent_bus.adapters', 'agent_bus.protocol', 'adapter_check',
           'agent_bus_cli', 'bus', 'client', 'local_config', 'projection_store',
           'recovery', 'runtime', 'scheduling', 'telemetry', 'operator_controls']
origins = {}
for name in modules:
    path = pathlib.Path(importlib.import_module(name).__file__).resolve()
    if not path.is_relative_to(prefix):
        raise RuntimeError(f'{name} imported outside isolated environment: {path}')
    origins[name] = str(path)
print(json.dumps({'version': importlib.metadata.version('agent-bus'),
                  'origins': origins, 'python': sys.version}, indent=2))
"""


def clean_environment() -> dict[str, str]:
    """Do not let the developer's bus config or import paths select live work."""
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(('AGENT_BUS_', 'PYTHON'))
        and key not in {'VIRTUAL_ENV', 'PIP_TARGET', 'PIP_PREFIX', 'PIP_USER'}
    } | {'PYTHONUNBUFFERED': '1', 'PYTHONNOUSERSITE': '1'}
    # Keep package-index proxy configuration, but never proxy this local trial.
    bypass = environment.get('NO_PROXY', environment.get('no_proxy', ''))
    environment['NO_PROXY'] = ','.join(filter(None, [bypass, '127.0.0.1', 'localhost', '::1']))
    environment['no_proxy'] = environment['NO_PROXY']
    return environment


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def check(wheel: Path, output: Path, constraints: Path | None = None) -> dict:
    if not wheel.is_file() or wheel.suffix != '.whl':
        raise ValueError(f'wheel not found: {wheel}')
    if constraints is not None and not constraints.is_file():
        raise ValueError(f'constraints not found: {constraints}')
    output.mkdir(parents=True, exist_ok=False)
    print(f'Evidence: {output}', flush=True)
    report = {'status': 'running', 'wheel': wheel.name}
    processes: list[subprocess.Popen] = []
    logs = []
    env = clean_environment()
    previous_handler = signal.getsignal(signal.SIGTERM)

    def interrupted(*_):
        raise KeyboardInterrupt('release check interrupted')

    signal.signal(signal.SIGTERM, interrupted)
    try:
        with tempfile.TemporaryDirectory(prefix='agent-bus-installed-check-') as tmp:
            root = Path(tmp).resolve()
            environment = root / 'venv'
            work = root / 'work'
            work.mkdir()

            def run(label: str, argv: list[str], timeout: int = 30,
                    expected_exit: int = 0) -> str:
                result = subprocess.run(argv, cwd=work, env=env, capture_output=True,
                                        text=True, timeout=timeout)
                with (output / 'commands.log').open('a', encoding='utf-8') as log:
                    log.write(f'[{label}] exit={result.returncode}\n{result.stdout}\n{result.stderr}\n')
                if result.returncode != expected_exit:
                    raise RuntimeError(f'{label} failed (exit {result.returncode}); see commands.log')
                return result.stdout

            def launch(label: str, args: list[str]) -> None:
                log = (output / f'{label}.log').open('w', encoding='utf-8')
                logs.append(log)
                processes.append(subprocess.Popen(
                    args, cwd=work, env=env, stdin=subprocess.DEVNULL,
                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True))

            def wait_for(label, predicate, timeout=60):
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    for process in processes:
                        if process.poll() is not None:
                            raise RuntimeError(f'process exited before {label}: {process.returncode}')
                    if predicate():
                        return
                    time.sleep(0.2)
                raise TimeoutError(f'timed out waiting for {label}')

            venv.EnvBuilder(with_pip=True, system_site_packages=False).create(environment)
            python = str(environment / 'bin/python')
            cli = str(environment / 'bin/agent-bus')
            install = [python, '-m', 'pip', 'install', '--disable-pip-version-check', str(wheel)]
            if constraints:
                install += ['--constraint', str(constraints)]
            run('install wheel', install, timeout=240)
            (output / 'dependencies.txt').write_text(
                run('resolved dependencies', [python, '-m', 'pip', 'freeze']), encoding='utf-8')
            run('dependency consistency', [python, '-m', 'pip', 'check'])
            imports = json.loads(run('import origins', [python, '-I', '-c', IMPORT_PROBE]))
            report['imports'] = imports
            version = run('version', [cli, '--version']).strip()
            if version != f"agent-bus {imports['version']}":
                raise AssertionError(f'CLI and wheel version differ: {version}')
            report['version'] = version
            # Exercise the captured released database against installed modules,
            # outside the checkout. -I keeps the test's directory off sys.path.
            upgrade_test = Path(__file__).resolve().parents[1] / 'tests/test_released_upgrade.py'
            run('released database upgrade', [python, '-I', str(upgrade_test), '-v'])
            report['released_upgrade_verified'] = True
            (work / 'check_example.py').write_text(
                "from agent_bus import Completed\nclass Agent:\n"
                " def execute(self, assignment):\n"
                "  print('adapter chatter must not reach JSON')\n"
                "  return Completed('probe passed')\n", encoding='utf-8')
            probe = json.loads(run('adapter probe', [cli, 'adapter', 'check',
                '--python-target', 'check_example:Agent', '--timeout', '10', '--json']))
            if probe.get('ok') is not True:
                raise AssertionError(probe)
            (work / 'hanging_example.py').write_text(
                "import time\nclass Agent:\n def __init__(self): time.sleep(60)\n",
                encoding='utf-8')
            timeout_probe = json.loads(run('adapter timeout', [cli, 'adapter', 'check',
                '--python-target', 'hanging_example:Agent', '--timeout', '1', '--json'],
                timeout=10, expected_exit=4))
            if timeout_probe.get('ok') is not False or timeout_probe['checks'][0]['name'] != 'probe timeout':
                raise AssertionError(timeout_probe)
            for name, value in [('adapter-check', probe), ('adapter-timeout', timeout_probe)]:
                (output / f'{name}.json').write_text(json.dumps(value, indent=2), encoding='utf-8')
            report['adapter_checks_verified'] = True
            run('init', [cli, 'init'])
            config_path = work / 'agent-bus.local.json'
            config = json.loads(config_path.read_text())
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            config['bus']['port'] = port
            config_path.write_text(json.dumps(config), encoding='utf-8')
            url = f'http://127.0.0.1:{port}'
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

            def ready():
                try:
                    with opener.open(url + '/health', timeout=2) as response:
                        return json.load(response).get('ok') is True
                except (urllib.error.URLError, TimeoutError):
                    return False

            try:
                launch('server', [cli, 'serve'])
                wait_for('healthy bus', ready)
                launch('pm', [cli, 'pm'])
                launch('worker', [cli, 'demo-worker', 'release-check'])
                submitted = json.loads(run('submit', [cli, 'submit', 'Verify packaged workflow', '--json']))
                task_id = str(submitted['payload']['task_id'])
                correlation_id = submitted['correlation_id']
                task = {}

                def completed():
                    nonlocal task
                    task = json.loads(run('task', [cli, 'task', task_id, '--json'], timeout=10))
                    if task['status'] in {'failed', 'cancelled', 'deadline_exceeded'}:
                        raise AssertionError(f'task unexpectedly terminal: {task}')
                    return task['status'] == 'completed' and not task['assignment_active']

                wait_for('completed task', completed)
                workflow = json.loads(run('workflow', [cli, 'workflow', correlation_id, '--json']))
                if workflow['status'] != 'completed' or workflow['task_count'] != 1:
                    raise AssertionError(workflow)
                doctor = json.loads(run('doctor', [cli, 'doctor', '--json']))
                if doctor.get('ok') is not True or doctor.get('healthy_workers', 0) < 1:
                    raise AssertionError(f'doctor checks failed: {doctor}')
                for name, value in [('task', task), ('workflow', workflow), ('doctor', doctor)]:
                    (output / f'{name}.json').write_text(json.dumps(value, indent=2), encoding='utf-8')
                launch('operator-worker', [python, str(Path(__file__).with_name('operator_trial_worker.py').resolve()),
                       '--url', url, '--offset-directory', str(work / 'operator-offsets')])
                interventions = []
                final_tasks = []
                for mode in ('block', 'fail', 'cancel'):
                    created = json.loads(run('operator submit', [cli, 'submit', f'Trial {mode}',
                        '--context', json.dumps({'mode':mode}), '--capability',
                        'never-available' if mode == 'cancel' else 'operator-trial', '--json']))
                    tid = str(created['payload']['task_id'])
                    observed = {}

                    def state_is(expected):
                        nonlocal observed
                        observed = json.loads(run('operator task', [cli, 'task', tid, '--json']))
                        if mode == 'block' and expected == 'blocked':
                            return observed['status'] == expected and observed['decision']['needed']
                        return observed['status'] == expected

                    wait_for(f'{mode} initial state', lambda: state_is(
                        'blocked' if mode == 'block' else 'failed' if mode == 'fail' else 'open'))
                    if mode == 'block':
                        listing = json.loads(run('decisions', [cli, 'decisions', '--json']))
                        row = next(row for row in listing['decisions'] if row['task_id'] == int(tid))
                        command = [cli, 'decide', tid, '--decision-event', str(row['decision_event_id']),
                                   '--value', '"staging"', '--json']
                    elif mode == 'fail':
                        command = [cli, 'retry', tid, '--failed-event', str(observed['status_event_id']),
                                   '--additional-retries', '1', '--reason', 'trial repair', '--json']
                    else:
                        command = [cli, 'cancel', tid, '--reason', 'trial stop', '--json']
                    first = json.loads(run('operator intent', command))
                    if first['accepted'] is not True:
                        raise AssertionError(first)
                    wait_for(f'{mode} settled', lambda: state_is('cancelled' if mode == 'cancel' else 'completed'))
                    duplicate = json.loads(run('repeat operator intent', command))
                    if not duplicate['duplicate'] or duplicate['event']['id'] != first['event']['id']:
                        raise AssertionError(duplicate)
                    if mode != 'cancel':
                        run('stale operator target', command + ['--idempotency-key', f'stale-{mode}'], expected_exit=2)
                    interventions.append({'first':first, 'duplicate':duplicate})
                    final_tasks.append(observed)
                listing = json.loads(run('tasks', [cli, 'tasks', '--json']))
                flows = json.loads(run('workflows', [cli, 'workflows', '--json']))
                pending = json.loads(run('remaining decisions', [cli, 'decisions', '--json']))
                if len(listing['tasks']) != 4 or len(flows['workflows']) != 4 or pending['decisions']:
                    raise AssertionError((listing, flows, pending))
                with opener.open(url + '/events?limit=1000', timeout=10) as response:
                    recorded = json.load(response)
                for topic in ('task.retry_requested', 'task.cancel_requested', 'decision.made'):
                    if sum(e['topic'] == topic for e in recorded) != 1:
                        raise AssertionError(f'duplicate intent: {topic}')
                decision_task = next(t['task_id'] for t in final_tasks if 'block' in t['title'])
                completion = next(e for e in recorded if e['topic'] == 'task.completed'
                                  and e['payload']['task_id'] == decision_task)
                if completion['payload']['result']['decision'] != 'staging':
                    raise AssertionError('human input was not propagated to the new assignment')
                (output / 'operator-trial.json').write_text(json.dumps({
                    'interventions': interventions, 'tasks': listing, 'workflows': flows,
                    'pending_decisions': pending, 'events': recorded}, indent=2), encoding='utf-8')
                report['operator_interventions_verified'] = True
                report.update(task_id=task_id, correlation_id=correlation_id, status='passed')
            finally:
                for process in reversed(processes):
                    stop_process(process)
                processes.clear()
    except BaseException as exc:
        report.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        for process in reversed(processes):
            stop_process(process)
        for log in logs:
            log.close()
        signal.signal(signal.SIGTERM, previous_handler)
        (output / 'summary.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(f"Release check: {report['status']}", flush=True)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wheel', required=True, type=Path)
    parser.add_argument('--constraints', type=Path)
    parser.add_argument('--output', type=Path, help='new evidence directory (must not exist)')
    args = parser.parse_args()
    output = args.output or Path(tempfile.mkdtemp(prefix='agent-bus-release-evidence-')) / 'results'
    try:
        check(args.wheel.resolve(), output.resolve(),
              args.constraints.resolve() if args.constraints else None)
    except (OSError, ValueError, RuntimeError, AssertionError, subprocess.SubprocessError) as exc:
        print(f'release check failed: {exc}', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
