"""Pre-0.13 agent-bus settings fail clearly instead of being ignored."""
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from logplane import cli as logplane_cli
from logplane.legacy import check_legacy_environment, legacy_environment_error
from logplane.local_config import LocalConfig


class LegacyEnvironmentTests(unittest.TestCase):
    def test_old_variables_are_named_with_their_replacements(self):
        message = legacy_environment_error({'AGENT_BUS_TOKEN': 's', 'AGENT_BUS_URL': 'u', 'HOME': '/'})
        self.assertIn('AGENT_BUS_TOKEN -> LOGPLANE_TOKEN', message)
        self.assertIn('AGENT_BUS_URL -> LOGPLANE_URL', message)
        self.assertIsNone(legacy_environment_error({'LOGPLANE_TOKEN': 's'}))
        with self.assertRaises(RuntimeError):
            check_legacy_environment({'AGENT_BUS_TOKEN': 's'})

    def test_cli_refuses_to_run_with_an_old_token(self):
        stderr = io.StringIO()
        with patch.dict('os.environ', {'AGENT_BUS_TOKEN': 'secret'}):
            status = logplane_cli.main(['serve'], stdout=io.StringIO(), stderr=stderr)
        self.assertEqual(2, status)
        self.assertIn('AGENT_BUS_TOKEN -> LOGPLANE_TOKEN', stderr.getvalue())


class LegacyConfigTests(unittest.TestCase):
    def test_old_config_name_gets_a_rename_hint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'agent-bus.local.json').write_text(json.dumps({}))
            with self.assertRaisesRegex(ValueError, 'rename it to logplane.local.json'):
                LocalConfig.from_file(root / 'logplane.local.json')

    def _legacy_root(self, directory):
        root = Path(directory)
        (root / 'agent-bus.local.json').write_text(json.dumps({
            'schema_version': 1, 'worker_lease_seconds': 20,
            'bus': {'host': '127.0.0.1', 'port': 8875, 'database_path': '.agent-bus/events.db'}}))
        return root

    def test_read_and_control_commands_do_not_fall_back_to_another_bus(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._legacy_root(directory)
            for argv in (['tasks'], ['task', '1'], ['cancel', '1', '--reason', 'r'],
                         ['retry', '1', '--reason', 'r', '--failed-event', '5', '--additional-retries', '1'],
                         ['decide', '1', '--decision-event', '3', '--value', '"yes"']):
                with self.subTest(argv=argv):
                    stderr = io.StringIO()

                    def unexpected_client(*args, **kwargs):
                        raise AssertionError('no bus should be contacted')

                    with patch.dict('os.environ', {'LOGPLANE_URL': 'http://127.0.0.1:8765'}):
                        status = logplane_cli.main(
                            argv, stdout=io.StringIO(), stderr=stderr,
                            client_factory=unexpected_client,
                            local_config_path=root / 'logplane.local.json')
                    self.assertEqual(2, status)
                    self.assertIn('rename it to logplane.local.json', stderr.getvalue())

    def test_explicit_url_still_selects_a_bus(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._legacy_root(directory)
            self.assertEqual('http://127.0.0.1:8875', logplane_cli._resolve_observer_url(
                'http://127.0.0.1:8875', local_config_path=root / 'logplane.local.json'))

    def test_init_refuses_to_start_fresh_beside_an_old_config(self):
        from logplane.local_config import initialize_local_config

        with tempfile.TemporaryDirectory() as directory:
            root = self._legacy_root(directory)
            with self.assertRaisesRegex(ValueError, 'not initializing'):
                initialize_local_config(root)
            self.assertFalse((root / 'logplane.local.json').exists())
            self.assertFalse((root / '.logplane').exists())

    def test_missing_config_without_old_file_suggests_init(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'run logplane init first'):
                LocalConfig.from_file(Path(directory) / 'logplane.local.json')


if __name__ == '__main__':
    unittest.main()
