"""An ordinary Python agent whose core logic knows nothing about the bus."""

from agent_bus import Completed


class ExistingAgent:
    def run(self, assignment):
        """Receive one bounded assignment and return one explicit outcome."""
        if assignment.context.get("agent_bus_conformance_probe"):
            return Completed("Python adapter contract is valid")
        return Completed(
            f"Processed: {assignment.goal}",
            {"handled_by": "minimal-python-agent"},
        )

    def cancel(self, assignment_id):
        # Real agents should signal their cooperative cancellation primitive.
        # The runtime still rejects late lifecycle output after ownership loss.
        return None
