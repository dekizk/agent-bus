"""Release-derived public contract baseline; additions may be compatible."""
import importlib
import io
import json
from pathlib import Path
import unittest

import agent_bus
import agent_bus_cli
from tests.test_cli import FakeObserver


BASELINE = json.loads((Path(__file__).parent / 'fixtures/compatibility/v0.11.0.json').read_text())


class CompatibilityTests(unittest.TestCase):
    def test_released_public_names_remain_importable(self):
        for module_name, names in BASELINE['exports'].items():
            module = importlib.import_module(module_name)
            self.assertTrue(set(names).issubset(module.__all__), module_name)
            for name in names:
                self.assertTrue(hasattr(module, name), f'{module_name}.{name}')

    def test_released_executor_messages_still_parse(self):
        assignment = agent_bus.parse_assignment_message(BASELINE['assignment_v1'], 1)
        self.assertEqual('task:1:attempt:1', assignment['assignment_id'])
        self.assertEqual({'status': 'completed', 'summary': 'done', 'result': {'value': 42}},
                         agent_bus.parse_outcome_message(BASELINE['outcome_v1'], 1))

    def test_assignment_fields_and_effect_identity_do_not_silently_change(self):
        assignment = agent_bus.AssignmentContext(
            correlation_id='compat-flow', task_id=1, assignment_id='task:1:attempt:1',
            assignment_event_id=2, attempt=1, goal='Compatibility probe',
            assignee='example', worker_instance_id='instance-1')
        message = agent_bus.assignment_message(assignment.to_dict(), 1)
        self.assertEqual(BASELINE['assignment_v1']['protocol'], message['protocol'])
        for key, value in BASELINE['assignment_v1']['assignment'].items():
            self.assertEqual(value, message['assignment'][key], key)

    def test_cli_json_and_read_only_exit_contract(self):
        for args, expected in [(['task', '1', '--json'], 0),
                               (['task', '9999', '--json'], 3),
                               (['workflow', 'flow-1', '--json', '--mermaid'], 2)]:
            stdout, stderr = io.StringIO(), io.StringIO()
            code = agent_bus_cli.main(args, stdout=stdout, stderr=stderr,
                                      clock=lambda: 101, client_factory=FakeObserver,
                                      local_config_path='/nonexistent/compatibility-config.json')
            self.assertEqual(expected, code)
            if code == 0:
                task = json.loads(stdout.getvalue())
                self.assertEqual(1, task['task_id'])
                self.assertEqual('completed', task['status'])
                self.assertIs(task['assignment_active'], False)
            else:
                self.assertTrue(stderr.getvalue())


if __name__ == '__main__':
    unittest.main()
