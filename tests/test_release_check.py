import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.check_release import clean_environment, stop_process, check


class ReleaseCheckTests(unittest.TestCase):
    def test_environment_does_not_inherit_developer_bus_or_import_targets(self):
        with patch.dict(os.environ, {'AGENT_BUS_URL': 'http://production',
                                    'AGENT_BUS_TOKEN': 'secret', 'PYTHONPATH': '/checkout',
                                    'PIP_TARGET': '/checkout', 'VIRTUAL_ENV': '/host'}):
            env = clean_environment()
        for key in ('AGENT_BUS_URL', 'AGENT_BUS_TOKEN', 'PYTHONPATH', 'PIP_TARGET', 'VIRTUAL_ENV'):
            self.assertNotIn(key, env)
        self.assertIn('127.0.0.1', env['NO_PROXY'])
        self.assertEqual(env['NO_PROXY'], env['no_proxy'])

    def test_missing_wheel_is_rejected_without_creating_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'results'
            with self.assertRaisesRegex(ValueError, 'wheel not found'):
                check(Path(tmp) / 'missing.whl', output)
            self.assertFalse(output.exists())

    def test_existing_evidence_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            wheel = Path(tmp) / 'fake.whl'
            wheel.touch()
            output = Path(tmp) / 'results'
            output.mkdir()
            with self.assertRaises(FileExistsError):
                check(wheel, output)

    def test_owned_process_is_stopped_and_repeated_cleanup_is_safe(self):
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                                   start_new_session=True)
        try:
            stop_process(process)
            self.assertIsNotNone(process.poll())
            stop_process(process)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


if __name__ == '__main__':
    unittest.main()
