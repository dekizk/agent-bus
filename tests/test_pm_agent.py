import fcntl
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pm_agent
from pm_agent import PMState, apply_event, plan_next_emission, reconcile


def event(
    event_id,
    topic,
    actor,
    payload,
    *,
    ts=100.0,
    caused_by=None,
    schema_version=2,
):
    return {
        "id": event_id,
        "ts": ts,
        "topic": topic,
        "actor": actor,
        "payload": payload,
        "caused_by": caused_by,
        "schema_version": schema_version,
        "idempotency_key": None,
    }


class FakeBus:
    def __init__(self, starting_id, ts=101.0):
        self.next_id = starting_id
        self.ts = ts
        self.events = []
        self.by_key = {}

    def publish(self, topic, payload, caused_by=None, idempotency_key=None):
        if idempotency_key in self.by_key:
            return self.by_key[idempotency_key]
        sent = event(
            self.next_id,
            topic,
            "pm",
            payload,
            ts=self.ts,
            caused_by=caused_by,
        )
        sent["idempotency_key"] = idempotency_key
        self.next_id += 1
        self.events.append(sent)
        self.by_key[idempotency_key] = sent
        return sent


def registered(event_id=1, name="alice", instance="alice-1", ts=100.0):
    return event(
        event_id,
        "agent.registered",
        name,
        {
            "name": name,
            "instance_id": instance,
            "capacity": 1,
            "capabilities": [],
        },
        ts=ts,
    )


def created(event_id=2, task_id=1, ts=100.0):
    return event(
        event_id,
        "task.created",
        "human",
        {"task_id": task_id, "title": "demo"},
        ts=ts,
    )


class PMLockTests(unittest.TestCase):
    def test_contended_lock_is_not_truncated(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "pm.lock"
            lock_path.write_text("existing-owner")

            with lock_path.open("r+") as held_lock:
                fcntl.flock(held_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch.object(pm_agent, "LOCK_PATH", lock_path):
                    with self.assertRaisesRegex(SystemExit, "another PM"):
                        with pm_agent.single_pm_lock():
                            self.fail("contended lock should not be acquired")

            self.assertEqual("existing-owner", lock_path.read_text())

    @unittest.skipUnless(hasattr(os, "O_NOFOLLOW"), "O_NOFOLLOW is required")
    def test_lock_refuses_symlink_without_modifying_target(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "target"
            target.write_text("do-not-truncate")
            lock_path = Path(directory) / "pm.lock"
            lock_path.symlink_to(target)

            with patch.object(pm_agent, "LOCK_PATH", lock_path):
                with self.assertRaisesRegex(SystemExit, "cannot safely open"):
                    with pm_agent.single_pm_lock():
                        self.fail("symlink lock should not be acquired")

            self.assertEqual("do-not-truncate", target.read_text())

    def test_new_lock_is_user_only(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "pm.lock"

            with patch.object(pm_agent, "LOCK_PATH", lock_path):
                with pm_agent.single_pm_lock():
                    mode = lock_path.stat().st_mode & 0o777

            self.assertEqual(0o600, mode)


class ReconciliationTests(unittest.TestCase):
    def test_restart_reconciles_assignment_missing_after_replay(self):
        history = [registered(), created()]
        state = PMState()
        for item in history:
            self.assertTrue(apply_event(state, item))

        bus = FakeBus(starting_id=3)
        emitted = reconcile(state, bus, now=101.0, lease_seconds=20)

        self.assertEqual(["task.assigned"], [item["topic"] for item in emitted])
        self.assertEqual("task:1:attempt:1", emitted[0]["payload"]["assignment_id"])

        restarted = PMState()
        for item in history + bus.events:
            apply_event(restarted, item)
        second_bus = FakeBus(starting_id=4)
        self.assertEqual([], reconcile(restarted, second_bus, now=102.0, lease_seconds=20))

    def test_restart_reconciles_missing_human_decision_request(self):
        log = [
            registered(),
            created(),
            event(
                3,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "attempt": 1,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                },
                caused_by=2,
            ),
            event(
                4,
                "task.blocked",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "reason": "need approval",
                },
                caused_by=3,
            ),
        ]
        state = PMState()
        for item in log:
            apply_event(state, item)

        bus = FakeBus(starting_id=5)
        emitted = reconcile(state, bus, now=101.0, lease_seconds=20)

        self.assertEqual(["decision.needed"], [item["topic"] for item in emitted])
        self.assertEqual("decision:task:1:attempt:1", emitted[0]["payload"]["decision_id"])
        self.assertEqual("need approval", emitted[0]["payload"]["reason"])

    def test_stale_worker_cannot_complete_reassigned_attempt(self):
        state = PMState()
        base = [
            registered(name="alice", instance="alice-1"),
            registered(event_id=2, name="bob", instance="bob-1"),
            created(event_id=3),
            event(
                4,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "attempt": 1,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                },
                caused_by=3,
            ),
            event(
                5,
                "task.assignment_expired",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                    "reason": "lease expired",
                },
                caused_by=4,
            ),
            event(
                6,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:2",
                    "attempt": 2,
                    "assignee": "bob",
                    "worker_instance_id": "bob-1",
                },
                caused_by=5,
            ),
        ]
        for item in base:
            self.assertTrue(apply_event(state, item))

        stale = event(
            7,
            "task.completed",
            "alice",
            {
                "task_id": 1,
                "assignment_id": "task:1:attempt:1",
                "worker_instance_id": "alice-1",
            },
            caused_by=4,
        )
        self.assertFalse(apply_event(state, stale))
        self.assertEqual("assigned", state.tasks[1].status)
        self.assertEqual("bob", state.tasks[1].assignee)

    def test_human_decision_creates_a_new_attempt(self):
        state = PMState()
        log = [
            registered(),
            created(),
            event(
                3,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "attempt": 1,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                },
                caused_by=2,
            ),
            event(
                4,
                "task.blocked",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "reason": "choose backend",
                },
                caused_by=3,
            ),
            event(
                5,
                "decision.needed",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "reason": "choose backend",
                },
                caused_by=4,
            ),
            event(
                6,
                "decision.made",
                "human",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "decision": "SQLite",
                },
                caused_by=5,
            ),
        ]
        for item in log:
            self.assertTrue(apply_event(state, item))

        planned = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual("task.assigned", planned["topic"])
        self.assertEqual(2, planned["payload"]["attempt"])
        self.assertEqual("task:1:attempt:2", planned["payload"]["assignment_id"])

    def test_worker_replacement_expires_old_attempt(self):
        state = PMState()
        for item in (
            registered(),
            created(),
            event(
                3,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "attempt": 1,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                },
                caused_by=2,
            ),
            registered(event_id=4, instance="alice-2", ts=101.0),
        ):
            apply_event(state, item)

        planned = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual("task.assignment_expired", planned["topic"])
        self.assertEqual("worker process was replaced", planned["payload"]["reason"])

    def test_malformed_event_does_not_poison_replay(self):
        state = PMState()
        malformed = event(1, "agent.registered", "alice", {})
        self.assertFalse(apply_event(state, malformed))
        self.assertEqual({}, state.workers)


if __name__ == "__main__":
    unittest.main()
