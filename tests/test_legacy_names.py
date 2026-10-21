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

    def test_missing_config_without_old_file_suggests_init(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'run logplane init first'):
                LocalConfig.from_file(Path(directory) / 'logplane.local.json')


if __name__ == '__main__':
    unittest.main()
