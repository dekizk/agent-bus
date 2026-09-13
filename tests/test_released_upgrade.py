"""Upgrade captured released history, not a database made by today's writer."""
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import agent_bus
import agent_bus_cli
import bus
from executors import outcome_from_dict
from observer import ObserverClient
from operator_controls import intervene
from pm_agent import plan_next_emission
from projection import replay_events
from projection_store import load_projection, save_projection
from runtime import WorkerRuntime


FIXTURE = Path(__file__).parent / 'fixtures/compatibility/v0.11.0-upgrade'
EXPECTED = json.loads((FIXTURE / 'expected.json').read_text())


class ReleasedUpgradeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'events.db'
        sql = (FIXTURE / 'database.sql').read_bytes()
        self.assertEqual(EXPECTED['sql_sha256'], hashlib.sha256(sql).hexdigest())
        with sqlite3.connect(self.path) as conn:
            conn.executescript(sql.decode())
            self.original_rows = conn.execute('SELECT * FROM events ORDER BY id').fetchall()
        patcher = patch.object(bus, 'DB_PATH', self.path)
        patcher.start()
        self.addCleanup(patcher.stop)
        auth = patch.object(bus, 'API_TOKEN', None)
        auth.start()
        self.addCleanup(auth.stop)
        context = TestClient(bus.app)
        self.server = context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        self.observer = ObserverClient('http://testserver', client=self.server)

    def assert_additive(self, expected, actual):
        """Public object fields may be added; existing values/types may not drift."""
        self.assertIs(type(expected), type(actual))
        if isinstance(expected, dict):
            self.assertTrue(expected.keys() <= actual.keys())
            for key in expected:
                self.assert_additive(expected[key], actual[key])
        elif isinstance(expected, list):
            self.assertEqual(len(expected), len(actual))
            for old, new in zip(expected, actual):
                self.assert_additive(old, new)
        else:
            self.assertEqual(expected, actual)

    def state(self):
        return replay_events(bus.fetch_after(0, None))

    def append(self, topic, actor, payload, **kwargs):
        with patch.object(bus.time, 'time', return_value=1001.0):
            return bus.append_event(topic, actor, payload, **kwargs)

    def drain(self, now=1001.0):
        emitted = []
        for _ in range(100):
            plan = plan_next_emission(self.state(), now=now,
                                     default_workflow_max_active_assignments=99,
                                     default_workflow_max_total_tokens=999999,
                                     default_workflow_reserve_tokens_per_assignment=1000)
            if plan is None:
                return emitted
            with patch.object(bus.time, 'time', return_value=now):
                emitted.append(bus.append_event(actor='pm', **plan))
        self.fail('upgraded PM did not settle')

    def assert_original_history(self):
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(self.original_rows, conn.execute(
                'SELECT * FROM events WHERE id<=? ORDER BY id',
                (len(EXPECTED['events']),)).fetchall())

    def test_startup_preserves_raw_history_identity_and_idempotency(self):
        for _ in range(2):
            bus.init_db()
            self.assertEqual(EXPECTED['events'], bus.fetch_after(0, None))
            self.assert_original_history()
        self.assertEqual('0000000000000000000000000000000b', self.observer.health()['database_id'])
        created = EXPECTED['events'][1]
        duplicate = self.append('task.created', 'human', created['payload'],
            correlation_id='upgrade-flow', idempotency_key=created['idempotency_key'])
        self.assertEqual(created, duplicate)
        with self.assertRaises(bus.IdempotencyConflict):
            self.append('task.created', 'human', {**created['payload'], 'title': 'changed'},
                correlation_id='upgrade-flow', idempotency_key=created['idempotency_key'])
        next_task = self.append('task.created', 'human', {'title': 'new intent'})
        self.assertEqual(9, next_task['payload']['task_id'])
        self.assertGreater(next_task['id'], EXPECTED['events'][-1]['id'])
        self.assert_original_history()

    def test_released_snapshot_and_rebuilt_indexes_preserve_next_pm_decision(self):
        cold = load_projection(self.path, use_snapshot=False)
        warm = load_projection(self.path)
        self.assertEqual(vars(cold.state), vars(warm.state))
        self.assertEqual(EXPECTED['next_plan'], plan_next_emission(warm.state, now=1001.0))
        # Snapshots may be ignored across Python/reducer implementations. Require
        # equivalent replay, not continued support for private cache bytes.
        with sqlite3.connect(self.path) as conn:
            conn.execute('DROP TABLE task_identity')
            conn.execute('DROP TABLE coordination_snapshot')
        bus.init_db()
        rebuilt = load_projection(self.path)
        self.assertEqual(vars(cold.state), vars(rebuilt.state))
        self.assertIsNone(rebuilt.snapshot_event_id)
        save_projection(self.path, rebuilt)
        self.assertEqual(vars(cold.state), vars(load_projection(self.path).state))
        self.assert_original_history()

    def test_released_cli_json_fields_types_and_exit_statuses(self):
        for case in EXPECTED['cli']:
            with self.subTest(argv=case['argv']):
                out, err = io.StringIO(), io.StringIO()
                code = agent_bus_cli.main(case['argv'], stdout=out, stderr=err,
                    clock=lambda: 1001.0, client_factory=lambda *a, **k: self.observer,
                    local_config_path=Path(self.temp.name) / 'no-config.json')
                self.assertEqual(case['exit'], code, err.getvalue())
                if code == 0:
                    self.assert_additive(case['json'], json.loads(out.getvalue()))
        self.assert_original_history()

    def test_pending_cancel_and_dag_result_continue_after_upgrade(self):
        emitted = self.drain()
        self.assertEqual('task.cancelled', emitted[0]['topic'])
        assigned = next(e for e in emitted if e['topic'] == 'task.assigned' and e['payload']['task_id'] == 2)
        completion = next(e for e in EXPECTED['events'] if e['topic'] == 'task.completed')
        self.assertEqual([{'task_id': 1, 'completion_event_id': completion['id']}],
                         assigned['payload']['dependency_refs'])
        resolved = WorkerRuntime._resolve_dependency_refs(
            SimpleNamespace(bus=SimpleNamespace(get_event=bus.fetch_event)), assigned)
        assignment = agent_bus.AssignmentContext.from_event(resolved)
        self.assertEqual(42, assignment.dependencies[0]['result']['value'])
        self.append('task.completed', 'fixture-worker', {'task_id': 2,
            'assignment_id': assignment.assignment_id, 'worker_instance_id': 'release-instance',
            'summary': 'continued after upgrade', 'result': {'value': 42}}, caused_by=assigned['id'])
        self.assertEqual('completed', self.state().tasks[2].status)
        self.assertEqual([], self.drain())
        self.assert_original_history()

    def test_released_human_decision_retry_and_pause_resume_continue(self):
        for task_id, command, options in [(3, 'decide', {'decision': 'staging'}),
                                         (4, 'retry', {'reason': 'repaired', 'additional_retries': 1})]:
            task = self.state().tasks[task_id]
            anchor = task.decision_event_id if command == 'decide' else task.failed_event_id
            result = intervene(self.observer, lambda intent: self.append(**intent), command=command,
                               task_id=task_id, anchor=anchor, now=1001.0, **options)
            self.assertTrue(result['accepted'])
        self.append('task.resume_requested', 'human', {'task_id': 5, 'reason': 'ready'})
        self.drain()
        self.assertTrue(any(self.state().tasks[i].status == 'open' for i in (3, 4, 5)))
        self.assertEqual(10000, self.state().workflows['upgrade-flow'].max_total_tokens)
        self.assertEqual(10, self.state().workflows['upgrade-flow'].max_active_assignments)
        # Unknown usage on earlier attempts remains charged. A changed process
        # default cannot relax the released policy; an explicit policy can.
        self.append('workflow.policy_set', 'human', {'source': 'operator', 'reason': 'grant more',
            'max_total_tokens': 20000, 'reserve_tokens_per_assignment': 1000},
            correlation_id='upgrade-flow')
        self.drain()
        for task_id in (3, 4, 5):
            task = self.state().tasks[task_id]
            self.assertEqual('assigned', task.status)
            self.assertEqual(2, task.attempt)
        self.assertEqual('staging', self.state().tasks[3].decisions[-1]['decision'])
        self.assert_original_history()

    def test_released_lease_and_deadline_expire_then_new_worker_continues(self):
        self.drain(now=1100.0)
        self.assertEqual('deadline_exceeded', self.state().tasks[8].status)
        self.assertEqual('open', self.state().tasks[7].status)
        with patch.object(bus.time, 'time', return_value=1100.0):
            bus.append_event('agent.registered', 'fixture-worker', {'name': 'fixture-worker',
                'instance_id': 'new-instance', 'capacity': 10, 'capabilities': []})
        self.drain(now=1100.0)
        task = self.state().tasks[7]
        self.assertEqual('assigned', task.status)
        self.assertEqual(2, task.attempt)
        self.assertEqual('new-instance', task.worker_instance_id)
        self.assert_original_history()

    def test_released_assignment_outcome_and_cancellation_wire_contracts(self):
        for case in EXPECTED['assignments']:
            assignment = agent_bus.AssignmentContext.from_event(bus.fetch_event(case['event_id']))
            self.assert_additive(case['message'], agent_bus.assignment_message(assignment.to_dict(), 1))
            self.assertEqual(case['effect_id'], assignment.effect_id('publish'))
        for message, typename in zip(EXPECTED['outcomes_v1'], EXPECTED['outcome_types']):
            decoded = agent_bus.parse_outcome_message(message, 1)
            self.assertEqual(typename, type(outcome_from_dict(decoded)).__name__)
            self.assertEqual(message, agent_bus.outcome_message(decoded, 1))
        self.assertEqual(EXPECTED['cancellation_v1'], agent_bus.cancellation_message('task:7:attempt:1'))


if __name__ == '__main__':
    unittest.main()
