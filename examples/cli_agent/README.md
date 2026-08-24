# Minimal CLI agent

The agent reads one versioned JSON request from standard input and writes one
versioned JSON outcome to standard output. Configuration supplies the command,
worker identity, capabilities, timeout, and protocol version; no shell is used.

From the repository root:

```sh
agent-bus adapter check --config examples/cli_agent/adapter.json
agent-bus adapter run examples/cli_agent/adapter.json
```

Run the bus and PM first. The conformance probe must not produce external side
effects. Exit code `75` means retryable process failure; other non-zero codes
mean permanent failure.
