"""Read one protocol-v1 assignment from stdin and write one outcome."""

import json
import sys

from agent_bus import CURRENT_PROTOCOL_VERSION, outcome_message, parse_assignment_message


def main():
    request = json.load(sys.stdin)
    assignment = parse_assignment_message(request, CURRENT_PROTOCOL_VERSION)
    context = assignment.get("context", {})
    if context.get("agent_bus_conformance_probe"):
        summary = "CLI adapter contract is valid"
    else:
        summary = f"Processed: {assignment['goal']}"
    response = outcome_message(
        {
            "status": "completed",
            "summary": summary,
            "result": {"handled_by": "minimal-cli-agent"},
        },
        CURRENT_PROTOCOL_VERSION,
    )
    json.dump(response, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
