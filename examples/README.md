# Agent examples

This directory contains optional examples of connecting existing agents to the
framework-neutral contract in `executors.py`. Nothing under `examples/` is
imported by the agent-bus core, and example-specific dependencies do not belong
in the core requirements files.

Each example should:

- implement the public `Executor` protocol;
- let `WorkerRuntime` own registration, leases, retries, and lifecycle events;
- keep agent-specific prompts, configuration, and cancellation here;
- use fake-agent tests so the standard suite needs no credentials or paid API;
- document the authority granted to the external agent;
- avoid adding agent-specific topics to the coordination log.

See [`hermes/`](hermes/) for the first reference integration.
