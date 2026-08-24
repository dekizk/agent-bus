import tempfile
import unittest
from pathlib import Path

import bus
from adoption import (
    AdoptionBridge,
    AdoptionMode,
    CanarySelector,
    ExecutionOwner,
    ExternalOrigin,
    decide_ownership,
)
from topics import COORDINATION_TOPICS, INTEGRATION_TOPICS


class DirectClient:
    def __init__(self, actor="bridge"):
        self.actor = actor

    def publish(self, topic, payload, **kwargs):
        return bus.append_event(topic, self.actor, payload, **kwargs)


class AdoptionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()
        self.bridge = AdoptionBridge(DirectClient())
        self.origin = ExternalOrigin("legacy-agent", "work-42")

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_controlled_mode_creates_bus_owned_task(self):
        event = self.bridge.adopt(
            origin=self.origin,
            title="Migrate an existing task",
            mode=AdoptionMode.CONTROLLED,
            context={"repository": "agent-bus"},
            required_capabilities=["python"],
            max_retries=1,
            deadline_at=2_000_000_000.0,
        )
        self.assertEqual("task.created", event["topic"])
        self.assertEqual(
            {"mode": "controlled", "owner": "agent-bus"},
            event["payload"]["ownership"],
        )
        self.assertEqual(self.origin.to_dict(), event["payload"]["external_origin"])
        self.assertEqual(2_000_000_000.0, event["payload"]["deadline_at"])
        self.assertIn("task_id", event["payload"])

    def test_deadline_must_be_a_positive_finite_timestamp(self):
        for value in (0, -1, True, float("inf"), float("nan")):
            with self.subTest(deadline_at=value):
                with self.assertRaisesRegex(ValueError, "deadline_at"):
                    self.bridge.adopt(
                        origin=self.origin,
                        title="Invalid deadline",
                        mode=AdoptionMode.CONTROLLED,
                        deadline_at=value,
                    )

    def test_shadow_mode_only_records_an_observation(self):
        event = self.bridge.adopt(
            origin=self.origin,
            title="Observe without controlling",
            mode=AdoptionMode.SHADOW,
        )
        self.assertEqual("integration.task_observed", event["topic"])
        self.assertEqual(
            {"mode": "shadow", "owner": "external"},
            event["payload"]["ownership"],
        )
        self.assertNotIn("task_id", event["payload"])
        self.assertNotIn(event["topic"], COORDINATION_TOPICS)
        self.assertIn(event["topic"], INTEGRATION_TOPICS)

    def test_canary_selection_is_stable_across_restarts(self):
        first = CanarySelector(37.5, namespace="rollout-one")
        restarted = CanarySelector(37.5, namespace="rollout-one")
        origins = [ExternalOrigin("legacy", f"task-{index}") for index in range(100)]
        self.assertEqual(
            [first.selects(origin) for origin in origins],
            [restarted.selects(origin) for origin in origins],
        )
        selected = sum(first.selects(origin) for origin in origins)
        self.assertGreater(selected, 0)
        self.assertLess(selected, len(origins))

    def test_canary_routes_selected_and_unselected_work(self):
        selected = self.bridge.adopt(
            origin=ExternalOrigin("legacy", "selected"),
            title="Selected task",
            mode=AdoptionMode.CANARY,
            selector=CanarySelector(100),
        )
        observed = self.bridge.adopt(
            origin=ExternalOrigin("legacy", "observed"),
            title="External task",
            mode=AdoptionMode.CANARY,
            selector=CanarySelector(0),
        )
        self.assertEqual("task.created", selected["topic"])
        self.assertEqual("integration.task_observed", observed["topic"])
        self.assertEqual("canary", selected["payload"]["ownership"]["mode"])
        self.assertEqual("external", observed["payload"]["ownership"]["owner"])

    def test_one_origin_cannot_silently_change_owner(self):
        shadow = self.bridge.adopt(
            origin=self.origin,
            title="Original decision",
            mode=AdoptionMode.SHADOW,
        )
        repeated = self.bridge.adopt(
            origin=self.origin,
            title="Original decision",
            mode=AdoptionMode.SHADOW,
        )
        self.assertEqual(shadow["id"], repeated["id"])

        with self.assertRaises(bus.IdempotencyConflict):
            self.bridge.adopt(
                origin=self.origin,
                title="Original decision",
                mode=AdoptionMode.CONTROLLED,
            )

    def test_origin_claim_prevents_dual_ownership_across_bridge_actors(self):
        self.bridge.adopt(
            origin=self.origin,
            title="Original decision",
            mode=AdoptionMode.SHADOW,
        )
        other_bridge = AdoptionBridge(DirectClient(actor="misconfigured-bridge"))
        with self.assertRaisesRegex(bus.IdempotencyConflict, "dual ownership"):
            other_bridge.adopt(
                origin=self.origin,
                title="Original decision",
                mode=AdoptionMode.CONTROLLED,
            )

    def test_canary_requires_selector(self):
        with self.assertRaisesRegex(ValueError, "requires a CanarySelector"):
            decide_ownership(AdoptionMode.CANARY, self.origin)
        self.assertEqual(
            ExecutionOwner.EXTERNAL,
            decide_ownership(AdoptionMode.SHADOW, self.origin).owner,
        )


if __name__ == "__main__":
    unittest.main()
