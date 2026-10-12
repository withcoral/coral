---
name: to-benchmark
description: Create a benchmark skill from an issue. The benchmark skill runs coding agents against main and an optional branch and reports whether the issue's behaviour improved.
disable-model-invocation: true
---

# To Benchmark

## Purpose

Create a benchmark skill for one issue. The benchmark skill runs coding agents headlessly against builds of Coral and reports whether the behaviour described in the issue improved.

## Input

An issue. Any of the following counts:

- A GitHub or Linear issue reference, passed as the argument.
- A spec file path, passed as the argument.
- The current conversation, when no argument is given.

## Output

A new directory `.agents/benchmark-skills/bench-<behaviour>/` containing:

- `SKILL.md`, produced by filling in [TEMPLATE.md](TEMPLATE.md).
- `results.jsonl`, empty. The benchmark skill appends one line per run and it is committed. Run output itself goes under `tasks/benchmarks/`, which git ignores, and is never committed.
- `run.py`, a copy of [run.py](run.py). The benchmark skill runs it once per repeat, and again to read each run's standard metrics. Edit the copy if an agent needs different flags.
- `plot.py`, a copy of [plot.py](plot.py). The benchmark skill runs it to write `plots.png` from `results.jsonl`. Edit the copy if the benchmark needs a different plot.

Claude Code and Codex runs load the arm's Coral plugin (`plugins/coral`, which holds the Coral skill), because users run agents with it installed and a branch may change it. OpenCode runs do not load the plugin, so they cannot measure changes made in the Coral skill. Users who connect only the MCP server never see the skill, so when a change targets agent behaviour, run each agent in two setups: with `--coral-plugin` and without it.

## Procedure

1. Obtain the issue text.
   - If a reference was passed, fetch the full body and all comments.
   - If no reference was passed, use the conversation.
   - If neither describes an agent behaviour, ask the user for the issue.

2. Write the task prompt.
   - The task prompt is the message a user would have sent that leads an agent into the behaviour the issue describes.
   - Write it as a user would write it. Do not describe the failure or how to avoid it.
   - Agents run with their default tools, so the prompt alone has to lead them to Coral and into the behaviour. When a smoke run shows an agent answering another way, or never showing the behaviour, the benchmark skill reports back with a revised prompt; expect to revise.
   - Each run produces its own failure. Runs are expected to fail in different ways. The repeat count accounts for this.
   - If it is unclear what request leads to the behaviour, or more than one plausible request exists, ask the user before continuing.

3. Write the comparison and the metrics to track.
   - The comparison is one sentence: what we want to know about main versus the branch.
   - The metrics to track are the values recorded per run that the comparison is computed from. Each is yes/no or a number, read from the transcript. Progress is counted in tool calls, never turns, since transcripts do not mark model rounds.
   - [EXAMPLE.md](EXAMPLE.md) is the template filled in for one issue. There, the comparison is recovery rate and speed after a Program failure, and the metrics to track are: hit a Program failure, recovered, calls to recover, execs to recover, correct final answer.

4. Confirm with the user.
   - Show the task prompt, the comparison, and the metrics to track. These are developed with the user, so expect changes.
   - Ask which agents to run. Offer Claude Code, Codex, and OpenCode as the default.
   - For each agent, ask which model and reasoning effort to use. Offer `claude-opus-5-5` for Claude Code and `gpt-6.1-sol` for Codex, at medium effort, as the default. `gpt-6.1-sol` needs Codex CLI 0.160.0 or later; see KNOWN_ISSUES.md.
   - Ask how many repeats per agent per arm. Offer 5 as the default.
   - Ask how the repeats are scheduled. Offer one lane per agent per arm, lanes in parallel, repeats within a lane in sequence, as the default.
   - Revise until approved.

5. Write the benchmark skill.
   - Name the skill after the behaviour, for example `bench-program-failure-recovery`.
   - Copy TEMPLATE.md, fill in every placeholder, including the agents with their models and reasoning efforts, the repeat count, and the schedule, and save it as `.agents/benchmark-skills/bench-<behaviour>/SKILL.md`.
   - Create an empty `results.jsonl` beside it.
   - Copy `run.py` and `plot.py` beside it.
   - Keep the template's standard metrics and report format unless the issue or the user gives a reason to change them.
   - Delete the Execution bullet for any agent the user did not choose.

6. Report to the user.
   - State the skill's path.
   - State that running it without a target produces the baseline for main, and that a target can be a branch, a commit, or a pull request number.

## Reference

[EXAMPLE.md](EXAMPLE.md) is TEMPLATE.md filled in for withcoral/lagoon#122.

