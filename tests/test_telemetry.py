import tempfile
import unittest
from pathlib import Path

import bus
from artifacts import ArtifactStore
from executors import AssignmentContext
from telemetry import BusTelemetrySink, ProducerIdentity
from topics import COORDINATION_TOPICS, TELEMETRY_TOPICS


def assignment(**overrides) -> AssignmentContext:
    fields = dict(
        correlation_id="workflow-one",
        task_id=1,
        assignment_id="task:1:attempt:1",
        assignment_event_id=7,
        attempt=1,
        goal="demo",
        assignee="hermes",
        worker_instance_id="worker-1",
    )
    fields.update(overrides)
    return AssignmentContext(**fields)


class FakeBus:
    actor = "hermes"

    def __init__(self):
        self.events = []

    def publish(self, topic, payload, **kwargs):
        event = {
            "id": len(self.events) + 100,
            "topic": topic,
            "payload": payload,
            **kwargs,
        }
        self.events.append(event)
        return event


class TelemetrySinkTests(unittest.TestCase):
    def test_content_capture_is_off_by_default(self):
        fake = FakeBus()
        sink = BusTelemetrySink(
            fake,
            producer=ProducerIdentity("examples.hermes", "worker-1", "0.6.0"),
        )

        started = sink.model_started(
            assignment(),
            invocation_id="task:1:attempt:1:model:1",
            provider="nous",
            model="openai/gpt-5.5",
            input_content="do not put me in sqlite",
        )
        sink.model_completed(
            assignment(),
            invocation_id="task:1:attempt:1:model:1",
            provider="nous",
            model="openai/gpt-5.5",
            duration_ms=12.5,
            usage={"total_tokens": 10},
            output_content="or me",
            caused_by=started["id"],
        )

        serialized = str(fake.events)
        self.assertNotIn("do not put me", serialized)
        self.assertNotIn("or me", serialized)
        self.assertEqual([], fake.events[0]["payload"]["artifacts"])
        self.assertEqual(100, fake.events[1]["caused_by"])
        self.assertEqual(
            "worker-1", fake.events[0]["producer"]["instance_id"]
        )

    def test_opt_in_capture_writes_only_content_references_to_events(self):
        with tempfile.TemporaryDirectory() as directory:
            fake = FakeBus()
            store = ArtifactStore(Path(directory))
            sink = BusTelemetrySink(
                fake,
                producer=ProducerIdentity("agent", "worker-1"),
                artifact_store=store,
                capture_content=True,
            )
            sink.tool_started(
                assignment(),
                tool_call_id="call-1",
                tool_name="search",
                input_content={"query": "sensitive"},
            )

            reference = fake.events[0]["payload"]["artifacts"][0]
            self.assertEqual("tool_input", reference["kind"])
            self.assertEqual(b'{"query":"sensitive"}', store.get_bytes(reference))
            self.assertNotIn("sensitive", str(fake.events[0]))


class TelemetryBusContractTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_telemetry_is_validated_inherited_and_queryable_without_pm(self):
        root = bus.append_event(
            "task.created",
            "human",
            {"title": "telemetry test"},
            correlation_id="workflow-one",
        )
        event = bus.append_event(
            "telemetry.model.started",
            "hermes",
            {
                "task_id": root["payload"]["task_id"],
                "assignment_id": "task:1:attempt:1",
                "worker_instance_id": "worker-1",
                "invocation_id": "invocation-1",
                "provider": "nous",
                "model": "openai/gpt-5.5",
                "attributes": {},
                "artifacts": [],
            },
            caused_by=root["id"],
            idempotency_key="telemetry:model:invocation-1:started",
            producer={
                "implementation": "examples.hermes",
                "instance_id": "worker-1",
                "version": "0.6.0",
            },
        )

        self.assertEqual("workflow-one", event["correlation_id"])
        self.assertEqual("examples.hermes", event["producer"]["implementation"])
        self.assertEqual([event], bus.fetch_after(0, list(TELEMETRY_TOPICS)))
        self.assertEqual(
            ["task.created"],
            [
                item["topic"]
                for item in bus.fetch_after(0, list(COORDINATION_TOPICS))
            ],
        )

        with self.assertRaisesRegex(bus.EventValidationError, "require producer"):
            bus.append_event(
                "telemetry.model.started",
                "hermes",
                event["payload"],
                caused_by=root["id"],
            )

        for invalid_payload in (
            {**event["payload"], "prompt": "must not be inline"},
            {**event["payload"], "invocation_id": "x" * 129},
        ):
            with self.subTest(payload=invalid_payload):
                with self.assertRaises(bus.EventValidationError):
                    bus.append_event(
                        "telemetry.model.started",
                        "hermes",
                        invalid_payload,
                        caused_by=root["id"],
                        producer=event["producer"],
                    )

        with self.assertRaisesRegex(bus.EventValidationError, "duration_ms"):
            bus.append_event(
                "telemetry.model.completed",
                "hermes",
                {
                    **event["payload"],
                    "duration_ms": float("nan"),
                    "usage": {},
                },
                caused_by=event["id"],
                producer=event["producer"],
            )

    def test_bus_sink_round_trips_idempotent_model_and_tool_lifecycles(self):
        root = bus.append_event(
            "task.created",
            "human",
            {"title": "sink round trip"},
            correlation_id="workflow-one",
        )

        class DirectBus:
            actor = "hermes"

            @staticmethod
            def publish(topic, payload, **kwargs):
                return bus.append_event(topic, "hermes", payload, **kwargs)

        context = assignment(
            task_id=root["payload"]["task_id"],
            assignment_event_id=root["id"],
        )
        sink = BusTelemetrySink(
            DirectBus(),
            producer=ProducerIdentity("examples.hermes", "worker-1", "0.6.0"),
        )
        started = sink.model_started(
            context,
            invocation_id="invocation-1",
            provider="nous",
            model="openai/gpt-5.5",
        )
        retried = sink.model_started(
            context,
            invocation_id="invocation-1",
            provider="nous",
            model="openai/gpt-5.5",
        )
        model_done = sink.model_completed(
            context,
            invocation_id="invocation-1",
            provider="nous",
            model="openai/gpt-5.5",
            duration_ms=12,
            usage={"total_tokens": 10},
            caused_by=started["id"],
        )
        tool_started = sink.tool_started(
            context,
            tool_call_id="call-1",
            tool_name="clarify",
            invocation_id="invocation-1",
            caused_by=started["id"],
        )
        tool_done = sink.tool_completed(
            context,
            tool_call_id="call-1",
            tool_name="clarify",
            duration_ms=3,
            invocation_id="invocation-1",
            caused_by=tool_started["id"],
        )

        self.assertEqual(started["id"], retried["id"])
        self.assertEqual(started["id"], model_done["caused_by"])
        self.assertEqual(tool_started["id"], tool_done["caused_by"])
        self.assertEqual(
            4,
            len(bus.fetch_after(0, list(TELEMETRY_TOPICS))),
        )


if __name__ == "__main__":
    unittest.main()
