# Issue tracker: GitHub

Issues and specs for this repository live in GitHub Issues in
`withcoral/coral`. Use the `gh` CLI for all operations.

Always specify `--repo withcoral/coral`. This clone also has an `upstream`
remote for `withcoral/coral`, so implicit repository selection is not safe.

## Conventions

- Create: `gh issue create --repo withcoral/coral --title "..." --body "..."`.
  Use a heredoc for multiline bodies.
- Read: `gh issue view <number> --repo withcoral/coral --json number,title,body,labels,comments`.
- List: `gh issue list --repo withcoral/coral --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'`,
  with `--label` and `--state` filters as needed.
- Make a sub-issue: `gh issue create --repo withcoral/coral --parent <parent> ...`,
  or `gh issue edit <parent> --repo withcoral/coral --add-sub-issue <child>`.
- Blocking: use GitHub's native issue dependencies, which the GitHub UI shows.
  Add an edge with `gh issue create --repo withcoral/coral --blocked-by <n>,<n> ...`
  or `gh issue edit <child> --repo withcoral/coral --add-blocked-by <blocker>`.
  A ticket is unblocked when every blocker is closed.
- Count open blockers: `gh api repos/withcoral/coral/issues/<n> --jq .issue_dependencies_summary.blocked_by`.
  It counts open blockers only, so 0 means the ticket can start.
  `gh issue view <n> --json blockedBy` also lists closed blockers.
- Comment: `gh issue comment <number> --repo withcoral/coral --body "..."`
- Add or remove labels: `gh issue edit <number> --repo withcoral/coral --add-label "..."` or `--remove-label "..."`
- Close: post the explanation with `gh issue comment`, then run
  `gh issue close <number> --repo withcoral/coral`. On an issue that is
  already closed, `gh issue close --comment` drops the comment and still exits 0.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## When a skill says "publish to the issue tracker"

Create a GitHub issue in `withcoral/coral`.

## When a skill says "fetch the relevant ticket"

Read it as in **Read** above.

## Wayfinding operations

The map is one issue with child issues as tickets.

- Map: one issue labelled `wayfinder:map`, holding the Notes, Decisions-so-far
  and Fog body.
- Child ticket: a sub-issue of the map, labelled `wayfinder:research`,
  `wayfinder:prototype`, `wayfinder:grilling` or `wayfinder:task`.
- Blocking: see **Blocking** under Conventions.
- Frontier: list the map's open sub-issues. Drop any with an open blocker or an
  assignee. The first in map order wins.
- Claim: `gh issue edit <n> --repo withcoral/coral --add-assignee @me`,
  as the session's first write.
- Resolve: post the answer as a comment, close the issue, then append a context
  pointer (the gist and a link) to the map's Decisions-so-far.
