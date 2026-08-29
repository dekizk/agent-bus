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

Copyable examples:

- [`python_agent/`](python_agent/) wraps a Python object with `run()`;
- [`cli_agent/`](cli_agent/) uses the versioned stdin/stdout protocol;
- [`http_agent/`](http_agent/) demonstrates the guarded HTTP bridge, retry-safe
  effect identity, and cooperative cancellation;
- [`hermes/`](hermes/) is the realistic model-agent integration.
