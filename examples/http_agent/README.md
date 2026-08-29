# Minimal HTTP agent

This loopback-only FastAPI example shows how an agent in another process can
accept the public protocol-v1 assignment envelope and return a typed outcome.
It also exposes deliberately visible observations for normal completion, a
retry after an HTTP 503, retry-stable effect identity, and cooperative
cancellation.

From the repository root, start the example agent:

```sh
python -m uvicorn examples.http_agent.agent:app \
  --host 127.0.0.1 \
  --port 9000
```

In another terminal, verify its contract without a bus or PM:

```sh
agent-bus adapter check --config examples/http_agent/adapter.json
```

With the bus and PM running, connect it as a controlled worker:

```sh
agent-bus adapter run examples/http_agent/adapter.json
```

Tasks requiring `http-example` may set `context.trial_mode` to `normal`,
`retry_once`, or `cancel`. The current demonstration state is visible at
`http://127.0.0.1:9000/observations` and can be cleared by restarting the
example process.

## Safety boundary

The example validates the selected protocol and all identity headers. Its
conformance path performs no simulated effect. Normal and retry modes combine
the retry-stable effect scope with a fixed operation name, while cancellation
cooperates with the adapter's `/cancel` request.

The effect ledger is intentionally in memory so the behavior is easy to see.
A production HTTP agent must keep its idempotency/effect records in durable
storage and make the effect record and external operation atomic where the
downstream system permits it. Agent-bus owns assignment authority and suppresses
late lifecycle output; the external agent still owns safe execution of any
irreversible effects it performs.
