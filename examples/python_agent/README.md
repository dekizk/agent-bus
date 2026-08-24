# Minimal Python agent

`ExistingAgent` contains ordinary `run(assignment)` logic. The wrapper owns
bus registration, leases, event publishing, cancellation fencing, and retries.

From the repository root:

```sh
agent-bus adapter check --python-target examples.python_agent.agent:ExistingAgent
python -m examples.python_agent.run_worker
```

The check sends a side-effect-free probe. An adapter must recognize that probe
and avoid external effects. See the main README for the complete local startup
and task-submission sequence.
