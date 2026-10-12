# Issue tracker: GitHub

Issues and specs for this repository live in GitHub Issues in
`withcoral/coral`. Use the `gh` CLI for all operations.

Always specify `--repo withcoral/coral`. This clone also has an `upstream`
remote for `withcoral/coral`, so implicit repository selection is not safe.

## Conventions

- Create: `gh issue create --repo withcoral/coral --title "..." --body "..."`
- Read: `gh issue view <number> --repo withcoral/coral --json title,body,comments --jq '.title, .body, (.comments[].body)'`.
  Outside a terminal, `--comments` prints the comments without the body.
- List: `gh issue list --repo withcoral/coral`
- Comment: `gh issue comment <number> --repo withcoral/coral --body "..."`
- Add or remove labels: `gh issue edit <number> --repo withcoral/coral`
- Close: `gh issue close <number> --repo withcoral/coral`

Use a heredoc for multiline issue bodies.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## When a skill says "publish to the issue tracker"

Create a GitHub issue in `withcoral/coral`.

## When a skill says "fetch the relevant ticket"

Run:

```sh
gh issue view <number> --repo withcoral/coral --json title,body,comments \
  --jq '.title, .body, (.comments[].body)'
```

## Wayfinding operations

The map is one issue with child issues as tickets.

- Label a map `wayfinder:map`.
- Label child issues `wayfinder:research`, `wayfinder:prototype`,
  `wayfinder:grilling`, or `wayfinder:task`.
- Use GitHub sub-issues for map membership.
- Use GitHub issue dependencies for blocking relationships.
- If these features are not available, use a task list in the map and a
  `Blocked by: #<number>` line in each child issue.
- Claim a ticket by assigning it to yourself.
- Resolve a ticket by adding its answer as a comment and closing the issue.
- Add the resolved decision to the map's Decisions-so-far section.
