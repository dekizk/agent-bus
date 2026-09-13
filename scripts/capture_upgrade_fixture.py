"""Maintainer-only capture using an extracted, pinned v0.11.0 release.

Run with --source pointing at a git archive of SOURCE_COMMIT. Consumers never
regenerate fixtures during tests: expected results must come from the release.
Only a temporary database is mutated. Output paths must not already exist.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from unittest.mock import patch
import uuid

SOURCE_COMMIT = 'c06f33ac68873140ec5eca98ef9e84dc2d3bf63b'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    # Prevent shell configuration from changing captured policy defaults.
    for key in list(os.environ):
        if key.startswith('AGENT_BUS_'):
            del os.environ[key]
    sys.path.insert(0, str(source))
    import bus
    import agent_bus_cli
    import agent_bus
    import pm_agent
    import projection
    import projection_store
    import version
    from executors import outcome_from_dict

    assert version.VERSION == '0.11.0'
    hashes = {}
    repo = Path(__file__).resolve().parents[1]
    for module in list(sys.modules.values()):
        filename = getattr(module, '__file__', None)
        if not filename or not Path(filename).resolve().is_relative_to(source):
            continue
        path = Path(filename).resolve()
        relative = path.relative_to(source).as_posix()
        released = subprocess.run(['git', '-C', str(repo), 'show', f'{SOURCE_COMMIT}:{relative}'],
                                  check=True, capture_output=True).stdout
        assert path.read_bytes() == released, relative
        hashes[relative] = hashlib.sha256(released).hexdigest()
    for module in (bus, agent_bus_cli, agent_bus, pm_agent, projection, projection_store):
        assert Path(module.__file__).resolve().is_relative_to(source)

    args.output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='agent-bus-release-fixture-') as tmp:
        bus.DB_PATH = Path(tmp) / 'events.db'
        with patch.object(bus.uuid, 'uuid4', return_value=uuid.UUID(int=11)):
            bus.init_db()

        def append(topic, actor, payload, **kwargs):
            with patch.object(bus.time, 'time', return_value=1000.0):
                return bus.append_event(topic, actor, payload, **kwargs)

        def state():
            return projection.replay_events(bus.fetch_after(0, None))

        def drain():
            for _ in range(100):
                plan = pm_agent.plan_next_emission(state(), now=1000.0)
                if plan is None:
                    return
                append(actor='pm', **plan)
            raise AssertionError('release reconciliation did not settle')

        append('agent.registered', 'fixture-worker', {
            'name': 'fixture-worker', 'instance_id': 'release-instance',
            'capabilities': [], 'capacity': 10}, idempotency_key='fixture:registered')
        for task_id, title in enumerate(('parent', 'dependent', 'question', 'failure',
                                         'paused', 'cancel', 'active', 'deadline'), 1):
            payload = {'task_id': task_id, 'title': title, 'retry_policy': {'max_retries': 2}}
            if task_id == 2:
                payload['depends_on'] = [1]
            if task_id == 8:
                payload['deadline_at'] = 1002.0
            append('task.created', 'human', payload, correlation_id='upgrade-flow',
                   idempotency_key=f'fixture:create:{task_id}')
        append('workflow.policy_set', 'human', {
            'source': 'operator', 'reason': 'pinned release policy', 'max_active_assignments': 10,
            'max_total_tokens': 10000, 'reserve_tokens_per_assignment': 1000},
            correlation_id='upgrade-flow', idempotency_key='fixture:policy')
        drain()

        def outcome(task_id, topic, **extra):
            task = state().tasks[task_id]
            append(topic, 'fixture-worker', {'task_id': task_id,
                   'assignment_id': task.assignment_id, 'worker_instance_id': 'release-instance',
                   **extra}, caused_by=task.assignment_event_id,
                   idempotency_key=f'fixture:outcome:{task_id}')

        outcome(3, 'task.blocked', reason='Which environment?')
        outcome(4, 'task.attempt_failed', retryable=False, failure_code='repair_required', reason='Repair me')
        append('task.pause_requested', 'human', {'task_id': 5, 'reason': 'operator inspection'})
        drain()
        projection_store.save_projection(bus.DB_PATH, projection_store.load_projection(bus.DB_PATH))
        outcome(1, 'task.completed', summary='parent result', result={'value': 42})
        append('task.cancel_requested', 'human', {'task_id': 6, 'reason': 'no longer needed'},
               idempotency_key='fixture:cancel:6')
        append('custom.upgrade_probe', 'fixture-worker', {'opaque': ['keep', {'exact': True}]})
        records = bus.fetch_after(0, None)
        current = state()
        assert [current.tasks[i].status for i in range(1, 9)] == [
            'completed', 'open', 'blocked', 'failed', 'paused', 'cancellation_requested', 'assigned', 'assigned']

        class Observer:
            def __init__(self, *args, **kwargs):
                pass

            def query_all(self, *, topics=None, correlation_id=None, **kwargs):
                return [e for e in records if (not topics or e['topic'] in topics)
                        and (correlation_id is None or e['correlation_id'] == correlation_id)]

            def close(self):
                pass

        cli = []
        for command in ([['task', str(i), '--json'] for i in range(1, 9)] +
                        [['workflow', 'upgrade-flow', '--json'], ['workers', '--json'],
                         ['explain', '3', '--json'], ['task', '999', '--json']]):
            out, err = io.StringIO(), io.StringIO()
            code = agent_bus_cli.main(command, stdout=out, stderr=err, clock=lambda: 1001.0,
                client_factory=Observer, local_config_path=Path(tmp) / 'no-config.json')
            cli.append({'argv': command, 'exit': code,
                        'json': json.loads(out.getvalue()) if code == 0 else None})
        outcomes = [{'status': 'completed', 'summary': 'done', 'result': {'value': 42}},
                    {'status': 'blocked', 'reason': 'Question'},
                    {'status': 'retryable_failure', 'code': 'temporary', 'reason': 'temporary'},
                    {'status': 'permanent_failure', 'code': 'permanent', 'reason': 'permanent'}]
        wire = [agent_bus.outcome_message(value, 1) for value in outcomes]
        with sqlite3.connect(bus.DB_PATH) as conn:
            sql = '\n'.join(conn.iterdump()) + '\n'
        (args.output / 'database.sql').write_text(sql, encoding='utf-8')
        manifest = {'source_release': 'v0.11.0', 'source_commit': SOURCE_COMMIT,
                    'source_sha256': hashes, 'sql_sha256': hashlib.sha256(sql.encode()).hexdigest(),
                    'events': records, 'cli': cli, 'outcomes_v1': wire,
                    'outcome_types': [type(outcome_from_dict(v)).__name__ for v in outcomes],
                    'cancellation_v1': agent_bus.cancellation_message('task:7:attempt:1'),
                    'next_plan': pm_agent.plan_next_emission(current, now=1001.0)}
        manifest['assignments'] = [
            {'event_id': e['id'], 'message': agent_bus.assignment_message(
                agent_bus.AssignmentContext.from_event(e).to_dict(), 1),
             'effect_id': agent_bus.AssignmentContext.from_event(e).effect_id('publish')}
            for e in records if e['topic'] == 'task.assigned']
        (args.output / 'expected.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
        print(f'Captured {len(records)} released events and {len(cli)} CLI cases: {args.output}')


if __name__ == '__main__':
    main()
