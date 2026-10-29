---
name: bench-program-failure-recovery
description: Benchmark recovery from Program failures across coding agents on main and an optional branch.
disable-model-invocation: true
---

# Benchmark: Program failure recovery

## Purpose

Measure how often, and how quickly, coding agents recover after a Program fails on a null property access, and whether they then complete the task correctly.

Source: https://github.com/withcoral/lagoon/issues/122

## Input

An optional target, passed as the argument: a branch name, a commit, or a pull request number.

- With a target: compare the baseline against the target. The baseline is main, except for a merged pull request, where it is the parent of the merge commit and the target is the merge commit.
- Without a target: run main alone and report a baseline.

"Main" and "branch" below mean the baseline arm and the target arm.

## Definitions

- **Arm**: one build of Coral under test. The baseline is always an arm. The target, if given, is the second arm.
- **Run**: one headless invocation of one agent with the task prompt against one arm.
- **Repeat**: one of the runs for a given agent and arm.
- **Lane**: the repeats of one agent on one arm, run in sequence.
- **Tool call**: one call by the agent to any tool, numbered from 1 across a run. The unit of progress for every agent, since neither transcript marks model rounds; a round that fires several calls in parallel counts each.
- **Comparison**: the one thing this benchmark exists to compare between arms.
- **Metric to track**: a value recorded per run that the comparison is computed from. Specific to this benchmark. Yes/no values are reported as the number of runs where they were yes. Numbers are reported as medians.
- **Standard metric**: a value recorded per run for every benchmark, such as tokens and wall time. Reported like a metric to track: yes/no values as counts, numbers as medians.

## Setup

Read `KNOWN_ISSUES.md` in `.agents/skills/to-benchmark/` first.

1. Create the results directory `tasks/benchmarks/program-failure-recovery/<date>/` at the repository root. `tasks/` is ignored by git. Run output can contain data from live sources and is never committed.
2. Check that each agent can use its model: run `run.py run` once per agent, as in Execution, with any `--mcp-url`, the main worktree's `--coral-plugin`, and the prompt "Reply with the word ok.", from an empty directory. Record each CLI's version in `versions.txt` in the results directory. If a model is not available, stop and report to the user. Do not substitute another model.
3. Build each arm in its own git worktree with `cargo build --locked -p coral --bin coral`. Record the commit SHA of each arm.
4. For each arm, copy the live Coral config directory to a new directory. Use the current `CORAL_CONFIG_DIR` if set, otherwise `.dev/state`. The live runtime may hold the database open, so copy it with `sqlite3 -readonly <live>/coral.db ".backup <copy>/coral.db"`, and copy `config.toml`, `credentials.key` and `state-format` beside it. To query the copy, open it as `sqlite3 "file:<copy>/coral.db?immutable=1"`; a plain read-only open fails. Do not stop or change the live runtime or its directory. Each arm uses its own copy for every run.
5. Edit each copy's `config.toml` so the arm serves loopback MCP without a bearer token: delete the `[auth]` section and its subsections, delete `public_url` under `listeners.mcp` and `listeners.auth`, and delete the whole `[listeners.web]` section. Give `listeners.grpc`, `listeners.mcp` and `listeners.auth` loopback `bind` addresses with ports of the arm's own. Keep `[otel]` and every other section unchanged; arm telemetry goes to the same collector as the live runtime.
6. Start each arm's `target/debug/coral` with `CORAL_CONFIG_DIR` set to its copy and `CORAL_LOCAL_WEB_URL` set to `http://127.0.0.1:<port>`, where the port is another unused port of the arm's own; nothing needs to serve it. Wait until the MCP port accepts connections. The MCP URL is `http://127.0.0.1:<mcp port>/workspaces/<workspace id>/mcp`; find the workspace ID as the `workspace_id` in the copy's `provider_records` for the Providers the task prompt needs. Record the MCP URL of each arm. If an arm fails to open the copied database because its schema differs, or there is no live config, provision that arm fresh with the Providers the task prompt needs. There is no headless way to install a Provider; Reef's add-Provider flow needs a browser. Instead, write an untracked test file in the arm's `crates/coral-mcp/tests/`, modelled on `credential_free_tracer.rs`, that provisions a Workspace and a Provider from an OpenAPI document, enables its tools, starts the local MCP listener, and sleeps. Leave out the tracer's `with_run_update_page_bytes` call; it sets a 4 KB page target for a paging test, and production uses the default. For a public upstream, a loopback caching proxy in place of the document's server URL gives both arms identical bytes without a credential.
7. Create a separate empty working directory for each run. Agents are run from these directories, never from the repository, so nothing in the repository influences them.
8. After the last run, stop every arm's runtime.

## Agents

- Claude Code, `claude-opus-5-5`, effort medium
- Codex, `gpt-6.1-sol`, effort medium
- OpenCode, `opencode-go/grok-4.6`, effort medium

Each agent is given one MCP server, the arm's MCP URL, and the arm's Coral plugin (`plugins/coral`, which holds the Coral skill), and otherwise runs as a user who installed Coral runs it: default tools, default sandbox, no fences. A run that reaches the data some other way is a finding about the task prompt, handled in the Run procedure. In the instructions below, `$MCP_URL` is the arm's URL.

## Execution

Run each repeat with `run.py` in this skill's directory, from the agents' working directory:

```
uv run <skill dir>/run.py run <agent> --mcp-url $MCP_URL --coral-plugin <arm worktree>/plugins/coral \
  --model <model> --effort <effort> \
  --workdir . --out <results directory>/<arm>/<agent>/<repeat> "<task prompt>"
```

It applies to every run: `timeout 15m`, wall time measured outside the agent, stdout and stderr captured, stdin closed, model and reasoning effort pinned and recorded in `meta.json`. A run that times out is recorded as a timeout and counted as a failure. Per agent it also:

- Claude Code: loads only this MCP server and the arm's Coral plugin, through `--plugin-dir`, and no user settings, hooks, skills, or CLAUDE.md. Keeps its default tools. MCP tools are deferred behind ToolSearch; Bash is not. Caps the run at 25 turns.
- Codex: runs from a scratch `CODEX_HOME` holding only the user's auth, and a scratch `HOME` whose `.agents/skills` holds only the arm's Coral skill, so no other plugins, skills or MCP servers load. Keeps its default tools, including the account's hosted connectors and web search. Passes the server's destructive and open-world gates; see `KNOWN_ISSUES.md` for the versions that honour them. Codex has no turn cap; the timeout is the only limit.
- OpenCode: loads a separate MCP configuration from the run output directory, pins the model and effort, and approves tool calls.

The transcript is `run.jsonl`. Read a run's standard metrics with:

```
uv run <skill dir>/run.py metrics <agent> <results directory>/<arm>/<agent>/<repeat>
```

It prints the standard metrics, a tally of every tool the agent called, every exec and wait result with its call number, state, and failure code, and the agent's final message.

## Task prompt

Send this text, unchanged, as the user message for every run:

```
For odoo/odoo pull request 51976, give me the clone URL of the source repository, the login of the branch owner, the branch name, and the head commit SHA.
```

The pull request comes from a fork that has since been deleted, so GitHub returns `head.repo` and `head.user` as null although the schema types them as objects. A Program that reads `head.repo.clone_url` fails with a TypeError. The supported answer is: clone URL unavailable, branch owner unavailable, branch `12.0`, head SHA `3cf7aba2e293417c6e8a6a4785624c8315ccbb7a`. The PR author's `ghost` login is not the branch owner.

## Run procedure

1. Smoke run: one repeat per agent on main, saved under `<results directory>/smoke/<arm>/<agent>/1`. A run whose tool call waits on a required action, such as an approval, cannot be answered unattended: report it as a prompt problem. Read it with `run.py metrics` and check three things before continuing. The agent answered through Coral: the tool tally shows Coral's exec or wait fetching the data, and no shell command, web search, hosted connector, or other MCP server fetched any. Reading the Coral skill file is not fetching data. The behaviour the task prompt is meant to trigger occurred. All obtainable standard metrics are present. Record Codex cost as `null`; its transcript does not report cost. Check every entry in `KNOWN_ISSUES.md` that applies. Delete an entry about the harness or a CLI when it no longer reproduces. Keep an entry about model behaviour, such as an agent reaching for a shell CLI, unless the smoke run shows it no longer happens and the counted runs confirm it. Add new problems with their workaround. An unexpectedly missing metric is a harness problem: fix the setup and repeat. The first two checks failing is a prompt problem: stop and report to the user which agent it was, what the agent did instead, quoting the tool tally or the Program, and a revised task prompt that promotes the behaviour. Continue with the prompt the user settles on. Smoke runs are not counted.
2. For each arm, for each agent, perform 5 repeats, scheduled one lane per agent per arm, lanes in parallel, repeats within a lane in sequence. The same schedule applies to both arms, since wall time is compared between them. Read each run with `run.py metrics` as it finishes and apply the first two smoke checks to it. A repeat that fails either is kept and recorded, with `answered_through_coral` or `hit_program_failure` set accordingly and only metrics that depend on the failed check set to `null`. Record `correct_final_answer` for every run. When no Program failure occurs, set only `recovered`, `calls_to_recover`, and `execs_to_recover` to `null`. If more than one repeat of an agent on an arm fails either check, stop the remaining repeats for that agent and report to the user as in the smoke run.
3. Save each run's files in `<results directory>/<arm>/<agent>/<repeat>`.
4. After all runs, append one line per run to `results.jsonl` in this skill's directory. See Results file.

## Standard metrics

Collected for every run of every benchmark. Record a value as unavailable if it cannot be obtained.

From `run.py metrics`:

- Input tokens
- Output tokens
- Cost
- Wall time
- Number of tool calls

From the transcript:

- Number of exec calls
- Number of exec or wait results with a failed status
- Answered through Coral: every fetch of data went through this MCP server's exec or wait. Yes/no, from the tool tally.

## Comparison

How often, and how quickly, agents recover from a Program failure and then answer correctly, on main versus the branch.

## Metrics to track

Specific to this benchmark. What has to be recorded per run to compute the comparison. Each is yes/no or a number, read from the transcript. All of them are plotted over time.

- `hit_program_failure`: an exec or wait result has failure code `program_rejected`. Yes/no.
- `recovered`: a later exec or wait result has state `succeeded` and returns the branch name or head SHA. Yes/no. Only for runs where `hit_program_failure` is yes. A successful Run that returns only a failure flag, an empty object, or key names does not count.
- `calls_to_recover`: tool calls from the first failed result to the first successful one, inclusive. Number. Only for runs where `recovered` is yes.
- `execs_to_recover`: exec calls from the first failed result to the first successful one, inclusive. Number. Only for runs where `recovered` is yes. The difference from `calls_to_recover` is the search and describe calls spent looking for the cause.
- `correct_final_answer`: the agent's final message reports the clone URL and branch owner as unavailable, the branch as `12.0`, and the head SHA as `3cf7aba2e293417c6e8a6a4785624c8315ccbb7a`. Yes/no. Recorded for every run, including runs that never failed. A final message that names `ghost` as the branch owner is no.

## Reading the transcript

For every metric to track:

1. Read the whole transcript.
2. Answer yes, no, or unknown. Unknown means the transcript does not say.
3. For yes, note the call number and quote the line that shows it.
4. Tool calls, tool results, and Program text are evidence. The agent's own summary is not, except for `correct_final_answer`, which is read from the final message alone.

Unknown is reported separately and left out of the totals.

## Results file

`results.jsonl` sits next to this file in `.agents/benchmark-skills/bench-program-failure-recovery/` and is committed. One JSON line per run, appended on every benchmark run, never rewritten. Fields:

```json
{"benchmark": "program-failure-recovery", "sha": "<full commit sha of the arm>", "branch": "<target as given, or main>",
 "date": "<ISO 8601 UTC>", "agent": "claude-code", "agent_version": "2.1.266", "model": "<model id>", "effort": "<effort>",
 "repeat": 1, "timeout": false,
 "tracked": {"hit_program_failure": true, "recovered": true, "calls_to_recover": 4, "execs_to_recover": 2, "correct_final_answer": true},
 "standard": {"input_tokens": 41000, "output_tokens": 3100, "cost_usd": 0.42, "wall_time_s": 210,
              "tool_calls": 12, "exec_calls": 3, "failed_results": 2, "answered_through_coral": true}}
```

`date` is the run's start time from `meta.json`, and `agent_version` is the CLI's bare version number, such as `2.1.290`. Keys are snake_case. A value that could not be obtained, or does not apply to the run, is `null`.

## Plot

After appending to `results.jsonl`, run from this skill's directory:

```
uv run plot.py results.jsonl plots.png hit_program_failure recovered calls_to_recover execs_to_recover correct_final_answer
```

Commit `plots.png` with `results.jsonl`. `plot.py` is this benchmark's own copy and may be edited. One panel per metric, each showing the value per agent for every SHA in date order, so it reads as a history.

## Report

Answer one question first: did the branch change the behaviour. Everything else is context.

### 1. Verdict

One sentence answering the comparison, all agents together: the metrics to track on main and on the branch, and whether the branch is better, worse, or no different. In baseline mode, give the main figure alone. Show `plots.png` under it.

### 2. Results

One table, all agents together. Metrics to track first, then standard metrics.

| Measure | Main | Branch | Change |
|---|---|---|---|
| hit_program_failure | | | |
| recovered | | | |
| calls_to_recover | | | |
| execs_to_recover | | | |
| correct_final_answer | | | |
| Answered through Coral | | | |
| Exec calls | | | |
| Failed results | | | |
| Input tokens | | | |
| Output tokens | | | |
| Cost | | | |
| Wall time | | | |
| Tool calls | | | |

Then one sentence on the standard metrics: did the branch make runs cheaper, dearer, or the same. A branch that fixes the behaviour but doubles tool calls is worth saying.

### 3. Per-agent breakdown

Same rows, one column pair per agent. Always show the metrics to track. Show a standard metric only if it moved more than 20% or moved in opposite directions for different agents.

| Measure | Claude main | Claude branch | Codex main | Codex branch | OpenCode main | OpenCode branch |
|---|---|---|---|---|---|---|
| hit_program_failure | | | | | | |
| recovered | | | | | | |
| calls_to_recover | | | | | | |
| execs_to_recover | | | | | | |
| correct_final_answer | | | | | | |

This shows whether every agent improved or only one did.

### 4. Judgement

One paragraph. Did the branch fix the behaviour. What the transcripts showed that the table does not. Any agent that answered without Coral or never showed the behaviour, and the prompt change that would promote it. A reminder that 5 repeats per agent only shows large effects, and one or two runs difference is noise.

### 5. Transcripts

Paths to one main run and one branch run that show the difference, and any run that did something odd.

### Baseline mode

Without a branch, produce sections 1 to 4 with a Main column only and no Change column. The judgement describes what agents do today. Same rows, so a later branch run lines up beside it.
