import unittest

import httpx
import bus
from client import BusClient, BusProtocolError
from runtime import WorkerRuntime
from projection import PMState, apply_event
from tests.test_pm_agent import registered, created, assigned
from tests.test_v010_admission import PollingWorkerBus, RecordingExecutor
from tests import test_v010_phase3_accounting as accounting


class PagedFixture:
    iter_events = BusClient.iter_events
    def __init__(self, pages):
        self.pages = iter(pages)
        self.calls = []
    def query(self, **kwargs):
        self.calls.append(kwargs)
        page = next(self.pages)
        if isinstance(page, Exception):
            raise page
        return page


class StreamingHistoryTests(unittest.TestCase):
    def test_lazy_pages_and_exact_boundary_ignore_later_publication(self):
        first, target = registered(), created(4)
        fixture = PagedFixture([[first], [target], [created(9)]])
        iterator = fixture.iter_events(through_id=4, page_size=1)
        self.assertEqual([], fixture.calls)
        self.assertEqual(first, next(iterator))
        self.assertEqual(1, len(fixture.calls))
        self.assertEqual([target], list(iterator))
        self.assertEqual([0, 1], [call['after_id'] for call in fixture.calls])

    def test_rejects_bad_pages_and_propagates_transport_errors(self):
        invalid = [None, {}, [None], [dict(registered(), id=True)],
                   [dict(registered(), id=0)], [dict(registered(), payload=[])],
                   [dict(registered(), topic='wrong')], [registered(), registered()]]
        for page in invalid:
            with self.subTest(page=page):
                fixture = PagedFixture([page])
                with self.assertRaises(BusProtocolError):
                    list(fixture.iter_events(through_id=9, topics=['agent.registered'], page_size=1))
        fixture = PagedFixture([[registered()], httpx.ConnectError('offline')])
        with self.assertRaises(httpx.ConnectError):
            list(fixture.iter_events(through_id=9, page_size=1))

    def test_repeated_cursor_between_pages_is_rejected(self):
        fixture = PagedFixture([[registered()], [registered()]])
        with self.assertRaises(BusProtocolError):
            list(fixture.iter_events(through_id=9, page_size=1))

    def test_runtime_agrees_with_full_replay_at_every_page_size(self):
        history = [registered(), created(), assigned(3, 1)]
        for size in (1, 2, 3, 1000):
            class Transport:
                actor = 'alice'
                def iter_events(self, **kwargs):
                    return BusClient.iter_events(self, page_size=size, **kwargs)
                def query(self, *, after_id, topics, limit):
                    return [e for e in history if e['id'] > after_id][:limit]
            runtime = WorkerRuntime(Transport(), name='alice', instance_id='alice-1', executor=None)
            self.assertTrue(runtime._assignment_is_accepted(history[-1]))
            expected = PMState()
            for event in history:
                apply_event(expected, event)
            self.assertEqual(vars(expected), vars(runtime._admission_state))

    def test_missing_target_and_changed_delivery_never_authorize_execution(self):
        for history in ([registered(), created()], [registered(), created(), assigned(3, 1)]):
            fixture = PagedFixture([history])
            runtime = WorkerRuntime(fixture, name='alice', instance_id='alice-1', executor=None)
            with self.assertRaises(ValueError):
                runtime._assignment_is_accepted(dict(assigned(3, 1), ts=999))


class LiveRuntimeStreamingTests(unittest.TestCase):
    def test_later_page_failure_stops_heartbeats_without_executing(self):
        fixture = accounting.Phase3CAccountingTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture._register(capacity=1)
        fixture._create('page-failure', 'task')
        # A syntactically deliverable event; admission fails before it can run.
        delivery = dict(assigned(100, 1), payload={**assigned(100, 1)['payload'], 'goal': 'do not run'})
        class Transport(PollingWorkerBus):
            def iter_events(self, **kwargs):
                return BusClient.iter_events(self, page_size=1, **kwargs)
            def query(self, *, after_id, topics, limit):
                if after_id:
                    raise httpx.ConnectError('injected second-page failure')
                return bus.fetch_after(0, topics, limit=limit)
        executor = RecordingExecutor()
        runtime = WorkerRuntime(Transport(), name='alice', instance_id='alice-1', executor=executor,
                                log=lambda _: None)
        runtime.run([delivery])
        self.assertTrue(runtime._heartbeat_stop.is_set())
        self.assertTrue(runtime._closed)
        self.assertEqual([], executor.assignments)


if __name__ == '__main__':
    unittest.main()
