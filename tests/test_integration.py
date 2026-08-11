import tempfile
import time
import unittest
from pathlib import Path

import bus
from adoption import AdoptionBridge, AdoptionMode, ExternalOrigin
from executors import Blocked, Completed, InProcessExecutor
from pm_agent import PMState, PM_TOPICS, apply_event, reconcile
from runtime import WorkerRuntime


class DirectClient:
    def __init__(self, actor):
        self.actor = actor

    def publish(self, topic, payload, **kwargs):
        return bus.append_event(topic, self.actor, payload, **kwargs)


class ExistingAgentIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_existing_python_agent_runs_without_owning_bus_plumbing(self):
        root = AdoptionBridge(DirectClient("legacy-bridge")).adopt(
            origin=ExternalOrigin("legacy-system", "ticket-42"),
            title="Execute an imported agent task",
            mode=AdoptionMode.CONTROLLED,
            context={"repository": "agent-bus", "ticket": 42},
            required_capabilities=["python"],
            max_retries=1,
        )

        worker_bus = DirectClient("alice")
        registration = worker_bus.publish(
            "agent.registered",
            {
                "name": "alice",
                "instance_id": "alice-integration",
                "capacity": 1,
                "capabilities": ["python"],
            },
            idempotency_key="registered:alice-integration",
        )
        state = PMState()
        for event in bus.fetch_after(0, list(PM_TOPICS)):
            self.assertTrue(apply_event(state, event))
        assignment = reconcile(
            state,
            DirectClient("pm"),
            now=time.time(),
        )[0]

        class ExistingAgent:
            def __init__(self):
                self.received = None

            def run(self, received):
                self.received = received
                return Completed(
                    "existing agent finished",
                    {"ticket": received.context["ticket"]},
                )

        agent = ExistingAgent()
        runtime = WorkerRuntime(
            worker_bus,
            name="alice",
            instance_id="alice-integration",
            executor=InProcessExecutor(agent),
            capacity=1,
            capabilities=["python"],
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.run([assignment])

        self.assertEqual(registration["id"], bus.fetch_after(0, ["agent.registered"])[0]["id"])
        self.assertEqual(42, agent.received.context["ticket"])
        self.assertEqual("ticket-42", agent.received.external_origin["task_ref"])
        self.assertEqual(root["correlation_id"], agent.received.correlation_id)

        replayed = PMState()
        for event in bus.fetch_after(0, list(PM_TOPICS)):
            apply_event(replayed, event)
        task = replayed.tasks[root["payload"]["task_id"]]
        self.assertEqual("completed", task.status)
        self.assertEqual(1, task.attempt)
        completed = bus.fetch_after(0, ["task.completed"])
        self.assertEqual({"ticket": 42}, completed[0]["payload"]["result"])

    def test_human_decision_reaches_the_replacement_attempt(self):
        root = bus.append_event(
            "task.created",
            "human",
            {
                "title": "Prepare release instructions",
                "context": {"release_target": None},
                "required_capabilities": ["release"],
                "retry_policy": {"max_retries": 0},
            },
        )
        worker_bus = DirectClient("alice")
        worker_bus.publish(
            "agent.registered",
            {
                "name": "alice",
                "instance_id": "alice-decision-test",
                "capacity": 1,
                "capabilities": ["release"],
            },
            idempotency_key="registered:alice-decision-test",
        )

        def replay_and_reconcile():
            state = PMState()
            for item in bus.fetch_after(0, list(PM_TOPICS)):
                apply_event(state, item)
            return state, reconcile(state, DirectClient("pm"), now=time.time())

        _, first_emissions = replay_and_reconcile()
        first_assignment = first_emissions[0]

        class DecisionAwareAgent:
            def __init__(self):
                self.received = []

            def run(self, assignment):
                self.received.append(assignment)
                if not assignment.decisions:
                    return Blocked("release target required")
                target = assignment.decisions[-1]["decision"]["release_target"]
                return Completed("release target received", {"release_target": target})

        agent = DecisionAwareAgent()

        def run_assignment(assignment):
            WorkerRuntime(
                worker_bus,
                name="alice",
                instance_id="alice-decision-test",
                executor=InProcessExecutor(agent),
                capacity=1,
                capabilities=["release"],
                heartbeat_seconds=100,
                log=lambda message: None,
            ).run([assignment])

        run_assignment(first_assignment)
        _, decision_emissions = replay_and_reconcile()
        decision_needed = decision_emissions[0]
        self.assertEqual("decision.needed", decision_needed["topic"])

        worker_bus = DirectClient("human")
        worker_bus.publish(
            "decision.made",
            {
                "task_id": root["payload"]["task_id"],
                "assignment_id": decision_needed["payload"]["assignment_id"],
                "decision_id": decision_needed["payload"]["decision_id"],
                "decision": {"release_target": "staging"},
            },
            caused_by=decision_needed["id"],
            idempotency_key=f"decision:{decision_needed['id']}",
        )

        worker_bus = DirectClient("alice")
        _, second_emissions = replay_and_reconcile()
        second_assignment = second_emissions[0]
        self.assertEqual("task.assigned", second_assignment["topic"])
        run_assignment(second_assignment)

        self.assertEqual((), agent.received[0].decisions)
        self.assertEqual(
            {"release_target": "staging"},
            dict(agent.received[1].decisions[-1]["decision"]),
        )
        replayed = PMState()
        for item in bus.fetch_after(0, list(PM_TOPICS)):
            apply_event(replayed, item)
        self.assertEqual("completed", replayed.tasks[root["payload"]["task_id"]].status)


if __name__ == "__main__":
    unittest.main()
