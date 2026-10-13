---
name: gist-my-func
description: Create a secret GitHub gist with a Coral saved function's source and declaration.
disable-model-invocation: true
argument-hint: "[function path, e.g. unifi.networkHealth]"
---

# Gist my function

Publish one Coral saved function as a secret gist with two files: the implementation and its TypeScript declaration.

## Steps

1. **Start a task.** Call `mcp__coral__start_task` with an intent like "Gist saved function <path>". Pass its `task_id`, and an `intent` where the tool asks for one, to every later Coral call.

2. **Pick the function.** The argument is the function path. Accept `unifi.networkHealth` or `coral.functions.unifi.networkHealth`. Keep a path that already starts with `coral.functions.`; prefix any other path with `coral.functions.`.
   With no argument, call `mcp__coral__search` with no `query` to list saved functions. Ask the user to pick one with `AskUserQuestion`, offering up to four `saved_function` hits by path (description as the option detail). Done when you hold one full call path.

3. **Fetch it.** Call `mcp__coral__describe` on the call path. Keep three fields from the result:
   - `source` → the implementation
   - `typescript_declaration` → the declaration
   - `description` → the gist description
   If describe fails or the path is not a saved function, report that, end the task as `failure`, and stop.

4. **Write the files.** Let `<name>` be the last path segment. In a fresh directory in the scratchpad (or a temp directory), write:
   - `<name>.ts`: `source`, verbatim
   - `<name>.d.ts`: `typescript_declaration`, verbatim
   - `desc.txt`: `<full call path>: <description>`, with no trailing newline
   Verbatim means the JSON string decoded (`\n` becomes a newline, `\"` becomes `"`) and nothing else changed: no reformatting, no fixes, no added comments. The gist is a record of what Coral holds, flaws included.

5. **Create the gist.** Coral's GitHub provider has no gist operation, so use the `gh` CLI. It creates secret gists by default:
   ```bash
   gh gist create --desc "$(cat '<dir>/desc.txt')" '<dir>/<name>.ts' '<dir>/<name>.d.ts'
   ```
   Never paste the description into the command: it is user-authored text, and backticks or `$(…)` in it would run. Single-quote the paths, since a name may contain `$`.
   Done when `gh` prints the gist URL.

6. **End the task.** Call `mcp__coral__end_task` with `success`, or `failure` if the gist was not created.

7. **Report.** Give the gist URL, the two file names, and note that the gist is secret: anyone with the link can view it.
