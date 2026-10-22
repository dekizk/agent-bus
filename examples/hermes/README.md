# Hermes Agent example

This optional example runs [Hermes Agent](https://github.com/NousResearch/hermes-agent)
as an interchangeable logplane executor. Hermes performs one assignment;
logplane remains responsible for ownership, worker leases, retry policy,
capacity, and lifecycle events.

Nothing in the logplane core imports this directory. Hermes is not added to
the project requirements, and the standard tests use a fake command rather than
credentials or paid inference.

## Safety model

Hermes `-z` one-shot mode is designed for scripts, but it automatically bypasses
interactive tool approvals. This example therefore defaults to all of these
guardrails:

- Hermes `--safe-mode`, which excludes personal rules, memory, plugins, hooks,
  and MCP configuration;
- an explicit provider and model;
- an explicit toolset, defaulting to `clarify` only;
- an existing working directory selected by the operator;
- bounded stdout, stderr, prompt, result, usage metadata, and wall-clock time;
- process-group cancellation when logplane revokes an assignment;
- rejection of `all`, `todo`, `delegation`, and `cronjob` toolsets.

Hermes v0.20 does not expose a maximum-turn option on its pure `-z` interface.
The example uses a hard process timeout instead of importing Hermes private
internals; if the public one-shot contract adds a turn limit later, it can be
passed through here.

Do not point the first trial at a production checkout or irreversible external
systems. Use a disposable directory. Prompt instructions are not an operating
system security boundary; if Hermes receives `file` or `terminal`, run it under
an OS/container sandbox appropriate for the task.

## Confirm Hermes configuration

Hermes must already be installed and authenticated. Read the provider and model
you intend to make explicit:

```sh
hermes --version
hermes config get model
hermes auth status YOUR_PROVIDER
```

## Run a real disposable smoke test

This command uses a temporary event database and directory, invokes one paid
Hermes request with only the `clarify` toolset, replays the event log, and
requires the task to finish as `completed` with one model telemetry start and
one terminal model telemetry event:

```sh
python -m examples.hermes.live_smoke \
  --provider YOUR_PROVIDER \
  --model YOUR_MODEL
```

It does not connect to or modify the repository's normal `events.db`.

To complete the v0.6 artifact trial with disposable, non-sensitive content,
persist and automatically verify the captured prompt/output references:

```sh
python -m examples.hermes.live_smoke \
  --provider YOUR_PROVIDER \
  --model YOUR_MODEL \
  --capture-content \
  --artifact-directory /tmp/logplane-hermes-v06-smoke-artifacts
```

The command fails unless it finds and integrity-checks exactly one model-input
and one model-output reference. Without `--capture-content`, it instead fails
if any content reference appears, preserving the default privacy boundary.

## Run the end-to-end workflow

The quickest way to see a real agent under logplane is one command. It starts
a throwaway bus and coordinator, runs Hermes as one worker and a small Python
"publisher" agent as another, and submits a three-task chain: Hermes
summarises what logplane is, Hermes drafts a short announcement from that
summary, and the publisher writes it to a file only after you approve. The
two Hermes calls are real, paid inference through your Hermes configuration;
their tokens and cost appear in `logplane workflow`.

```sh
python -m examples.hermes.workflow --provider nous --model qwen/qwen3.8-max-0902
```

Use the provider and model from `hermes config get model` and
`hermes auth status PROVIDER`. Options: `--yes` approves publishing without
asking, `--max-cost` sets the workflow cap (default $1.00), `--reasoning`
forwards a Hermes reasoning level, and `--dir` keeps the history in a chosen
directory. The run ends with the announcement, the workflow's real usage, and
the command to reopen its history.

The publisher is the only agent that performs an irreversible action, and it
is deliberately not a model. It returns `Blocked` until a human answers with
`logplane decide`, and its write is keyed by `assignment.effect_id`, so a
retried or overlapping attempt cannot publish twice. The publisher reports no
model usage, so the workflow's budget line charges each of its attempts the
configured reservation instead of guessing; the telemetry line shows the
Hermes usage alone.

`tests/test_hermes_example.py` runs this workflow against a fake Hermes
executable, so CI covers the whole flow without credentials or paid calls.

## Review a codebase with a fleet of reviewers

The bigger example fans a code review out to several Hermes workers and puts
the coordinator through what goes wrong in real runs:

```sh
python -m examples.hermes.code_review --provider nous --model qwen/qwen3.8-max-0902
```

By default it splits logplane's own source into 16 chunks of about 10 KB,
starts 4 Hermes reviewers, and submits one review task per chunk, a merge task
that depends on all of them, and a filing task that depends on the merge.
While the reviews run:

- one reviewer is killed with `kill -9` in the middle of a real model call;
  its lease expires, the chunk is reassigned, and the dead call's usage is
  never reported, so its attempt is charged its reservation;
- after the first review completes, a cost cap set at about 60% of the
  projected total stops new work part-way through; you raise it (or `--yes`
  does), and both policy events carry a reason;
- Hermes merges every chunk's findings into one ranked list;
- a Python "filer" agent asks which severities to file, records each chosen
  finding once in `findings.db` under a key stable across attempts, and writes
  `REVIEW.md`.

Point `--path` at your own repository to review it instead; `--include` picks
the files, `--max-chunks` and `--workers` set the size of the run, and
`--no-crash` or `--no-cap` skip those steps. Each chunk must fit logplane's
16 KB inline context limit, so `--chunk-bytes` stays at or below about 12000.
Cost scales with chunks and model: the defaults cost a few tens of cents on a
mid-priced model; a frontier model over a large repository can cost dollars.
Hermes sees only the chunk text in its prompt and runs with the `clarify`
toolset in safe mode, so nothing is read from disk or executed.

`tests/test_hermes_example.py` runs this example against a fake Hermes too,
including the crash and the cap.

## Run Hermes as a normal worker

Start the bus and PM as documented in the root README. Create a disposable
working directory, then start the example worker:

```sh
mkdir -p /tmp/logplane-hermes-trial

python -m examples.hermes.run_worker \
  --name hermes \
  --working-directory /tmp/logplane-hermes-trial \
  --provider YOUR_PROVIDER \
  --model YOUR_MODEL \
  --toolsets clarify \
  --capability hermes
```

For an explicit reasoning effort, add `--reasoning high` to the worker command.
The adapter forwards this as Hermes' per-invocation `--reasoning` option, including
in safe mode; changing saved Hermes user configuration is not needed. Omit it to
preserve Hermes' default behavior. Supported CLI levels are `none`, `minimal`,
`low`, `medium`, `high`, `xhigh`, `max`, and `ultra`; actual support depends on
the selected model and provider. Use a Hermes version with `--reasoning` in
`hermes --help`. This option belongs to `run_worker`, not the task submission
or the separate `live_smoke` command.

Publish a low-risk task in another terminal:

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{
    "topic":"task.created",
    "actor":"human",
    "idempotency_key":"hermes-example-1",
    "payload":{
      "title":"Summarize the supplied note",
      "context":{
        "text":"logplane coordinates work while agents remain replaceable"
      },
      "required_capabilities":["hermes"],
      "retry_policy":{"max_retries":1}
    }
  }'
```

The adapter asks Hermes to return exactly one of the standard executor JSON
outcomes. `WorkerRuntime` translates it into `task.completed`, `task.blocked`,
or `task.attempt_failed`. The adapter also writes bounded model identity,
duration, outcome type, and Hermes cost/token metadata to the separate
`telemetry.model.*` stream. The PM never subscribes to these topics.

Inspect telemetry for one workflow using the `correlation_id` returned by
`task.created`:

```sh
curl 'http://127.0.0.1:8765/events?topics=telemetry.model.started,telemetry.model.completed,telemetry.model.failed&correlation_id=YOUR_CORRELATION_ID'
```

With the v0.8 editable install, the normal inspection path no longer requires
raw event JSON:

```sh
logplane workflow YOUR_CORRELATION_ID
logplane task YOUR_TASK_ID
logplane explain YOUR_TASK_ID
logplane --json workflow YOUR_CORRELATION_ID
```

The workflow view combines the derived DAG state with token, cost, duration,
and model-span totals while keeping telemetry outside PM replay.

Prompts and model output are not captured by default. For a disposable,
non-sensitive trial only, explicitly enable content-addressed capture:

```sh
python -m examples.hermes.run_worker \
  --name hermes \
  --working-directory /tmp/logplane-hermes-trial \
  --provider YOUR_PROVIDER \
  --model YOUR_MODEL \
  --toolsets clarify \
  --capability hermes \
  --capture-content \
  --artifact-directory /tmp/logplane-hermes-artifacts
```

SQLite receives only immutable SHA-256 references. The referenced local files
can still contain sensitive task data, so redact before capture, use a private
directory, and remove only blobs you have proven are no longer referenced.

Hermes also receives resolved upstream completions through
`AssignmentContext.dependencies`. To trial the v0.5 DAG path, create an initial
task, then create a second task with `payload.depends_on` containing the first
task's id and the same capability. The PM waits for the first completion and
passes its summary/result to Hermes by immutable event reference; do not copy
the result into the second task's context.

## Expanding tool authority

For a disposable read-only-style repository review, you may explicitly enable
`file`. For code execution, `terminal` is also possible:

```sh
--toolsets file,terminal
```

Hermes one-shot mode bypasses approvals, and both toolsets can mutate the host.
Use a disposable copy and an external sandbox. The example intentionally does
not offer Hermes task-board, scheduler, or delegation toolsets because those
would create a second orchestration owner alongside logplane.

`--unsafe-user-config` disables Hermes safe mode and permits the user's normal
rules/plugins/hooks. It is an explicit escape hatch for later experiments, not
the recommended starting configuration.

## Adapting the pattern to another agent

Copy the structure, not the Hermes implementation. A different adapter only
needs to provide:

1. assignment-to-agent input translation;
2. agent invocation and cancellation;
3. bounded output handling;
4. translation into `Completed`, `Blocked`, `RetryableFailure`, or
   `PermanentFailure`.

Keep agent-specific dependencies and topics outside the core until repeated
integrations demonstrate a genuinely common abstraction.
