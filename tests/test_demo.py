"""The scripted demos: stage logic in isolation, then each scenario live."""

import io
import json
import os
import tempfile
import unittest
from pathlib import Path

import demo
from executors import AssignmentContext, Blocked, Completed


def assignment(task_id, *, attempt=1, context=None, dependencies=(), decisions=()):
    return AssignmentContext(
        correlation_id="flow-1",
        task_id=task_id,
        assignment_id=f"task:{task_id}:attempt:{attempt}",
        assignment_event_id=100 + attempt,
        attempt=attempt,
        goal=f"stage {task_id}",
        context=context or {},
        dependencies=tuple(dependencies),
        decisions=tuple(decisions),
        assignee="lead",
        worker_instance_id="lead-1",
    )


def decision(task_id, value, attempt=1):
    return {"event_id": 90, "actor": "human", "assignment_id": f"task:{task_id}:attempt:{attempt}",
            "decision_id": f"decision:task:{task_id}:attempt:{attempt}", "decision": value}


def dependency(task_id, outcome):
    return {"task_id": task_id, "completion_event_id": 50 + task_id,
            "summary": outcome.summary, "result": demo.plain(outcome.result)}


class ScenarioStageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.workdir = Path(self.directory.name)
        self.executor = demo.ScenarioExecutor("lead", self.workdir)

    def tearDown(self):
        self.directory.cleanup()

    def run_stage(self, task_id, stage, *, dependencies=(), decisions=(), attempt=1, **context):
        return self.executor.execute(assignment(
            task_id, attempt=attempt, dependencies=dependencies, decisions=decisions,
            context={"stage": stage, "work_seconds": 0, **context}))

    def test_question_blocks_until_a_decision_arrives(self):
        self.assertIsInstance(self.run_stage(1, "question", question="SQLite?"), Blocked)
        done = self.run_stage(1, "question", question="SQLite?", attempt=2,
                              decisions=[decision(1, "SQLite")])
        self.assertEqual({"decision": "SQLite"}, dict(done.result))

    def test_audit_pipeline_merges_triages_and_reports_through_real_results(self):
        reviews = [self.run_stage(2 + index, "review", contract=contract)
                   for index, contract in enumerate(demo.REVIEW_FINDINGS)]
        merged = self.run_stage(5, "dedupe", dependencies=[
            dependency(2 + index, review) for index, review in enumerate(reviews)])
        self.assertIsInstance(merged, Completed)
        findings = merged.result["findings"]
        self.assertEqual(3, len(findings))
        oracle = next(item for item in findings if item["root_cause"] == "stale-oracle-price")
        self.assertEqual(("PriceOracle.sol", "Router.sol"), tuple(oracle["locations"]))

        blocked = self.run_stage(6, "triage", dependencies=[dependency(5, merged)])
        self.assertIsInstance(blocked, Blocked)
        self.assertIn(oracle["id"], blocked.reason)
        triaged = self.run_stage(6, "triage", attempt=2, dependencies=[dependency(5, merged)],
                                 decisions=[decision(6, "High")])
        severities = {item["id"]: item["severity"] for item in triaged.result["findings"]}
        self.assertEqual("High", severities[oracle["id"]])

        report = self.run_stage(7, "report", dependencies=[dependency(6, triaged)])
        self.assertIsInstance(report, Completed)
        self.assertIn(oracle["id"], (self.workdir / demo.REPORT_FILE).read_text())

    def test_submission_is_idempotent_across_attempts(self):
        findings = {"findings": [{"id": f"AUD-{n}", "severity": "High", "title": f"t{n}"}
                                 for n in (1, 2)]}
        upstream = [{"task_id": 7, "completion_event_id": 70, "summary": "r", "result": findings}]
        first = self.run_stage(8, "submit", dependencies=upstream)
        second = self.run_stage(8, "submit", dependencies=upstream, attempt=2)
        self.assertEqual(["AUD-1", "AUD-2"], list(first.result["submitted"]))
        self.assertEqual([], list(second.result["submitted"]))
        self.assertEqual(["AUD-1", "AUD-2"], list(second.result["skipped"]))
        entries = demo.read_inbox(self.workdir / demo.INBOX_FILE)
        self.assertEqual(2, len({entry["effect_id"] for entry in entries}))
        self.assertEqual(2, len(entries))

    def test_unknown_stage_is_an_error(self):
        with self.assertRaises(ValueError):
            self.run_stage(1, "nonsense")


class DemoCommandTests(unittest.TestCase):
    def test_listing_and_unknown_scenarios(self):
        out = io.StringIO()
        self.assertEqual(0, demo.run(None, out=out))
        for name in demo.SCENARIOS:
            self.assertIn(name, out.getvalue())
        self.assertEqual(2, demo.run("nope", out=io.StringIO()))

    def test_refuses_a_non_empty_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "keep.txt").write_text("mine")
            out = io.StringIO()
            self.assertEqual(2, demo.run("recovery", out=out, directory=directory))
            self.assertIn("new or empty", out.getvalue())


@unittest.skipIf(os.name != "posix", "demo scenarios use POSIX process signals")
class LiveScenarioTests(unittest.TestCase):
    """Each scenario end to end with real processes, at a fast pace."""

    def play(self, name):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        out = io.StringIO()
        code = demo.run(name, out=out, pace=0.25, assume_yes=True,
                        directory=directory.name, keep_running=False)
        self.assertEqual(0, code, out.getvalue())
        self.assertIn("What just happened", out.getvalue())
        return Path(directory.name), out.getvalue()

    def test_recovery(self):
        _, output = self.play("recovery")
        self.assertIn("task 2 → bob (attempt 2)", output)

    def test_decision(self):
        _, output = self.play("decision")
        self.assertIn("human decision: SQLite", output)

    def test_audit_triage(self):
        workdir, output = self.play("audit-triage")
        entries = demo.read_inbox(workdir / demo.INBOX_FILE)
        self.assertEqual(3, len(entries))
        self.assertEqual(3, len({entry["effect_id"] for entry in entries}))
        self.assertEqual({1, 2}, {entry["attempt"] for entry in entries})
        self.assertIn("skipped 1 already submitted", output)


if __name__ == "__main__":
    unittest.main()
