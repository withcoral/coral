# Our development workflow

Agents make implementation faster. They do not remove the need for a process,
shared understanding, or engineering judgement.

**We use one team process, not a collection of individual processes.** You can
choose your model, reasoning level, and harness. The workflow and quality bar
stay the same. If the process needs to change, propose a change for everyone.

**You are responsible for the code you ship.** An agent writing it, reviewing
it, or passing tests does not transfer that responsibility. All of us own the
quality of the product.

## The short version

1. Check the proposed work against existing issues, the spec, and planned work.
2. Run `/grill-with-docs` to agree the problem, scope, and approach. Then ask the
   agent to create or update the source ticket. Share changes to product behaviour
   or shared contracts with the team before building. For larger efforts, share
   the map early and use `/to-spec` and `/to-tickets` to publish the build plan.
3. Run `/implement` on a working branch.
4. In a **new session**, run `/code-review` against `main`, with the source issue
   and any relevant additional context. Review the code yourself in parallel.
5. Evaluate the findings and the approach. Iterate on the implementation or
   restart with what you have learned.
6. Verify the result and create a PR. Link the source ticket and try adding a
   `/show-me` visual to explain the change. Attach a screen recording for UI
   features.

Large, uncertain efforts need `/wayfinder` before implementation. The sections
below explain when and why.

## 1. Check that this is work we need now

Before starting, read the existing issue or describe the problem if there is no
issue yet. Check whether later tickets already address it.

Give the agent the issue you are working on, if one exists, including relevant
comments.

## 2. Think before building

Settle the problem, expected behaviour, scope, and how you will verify it.
Participate in the discussion; the agent should not make product decisions on
your behalf. Scale the planning to the level of uncertainty:

| Where you are                                                                    | What to run                                                                                                                                                                                                      |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| You have a tiny, mechanical change with clear behaviour                          | Keep the scope check brief, then ask the agent to create or update the ticket. You do not need a full spec for a small fix.                                                                                      |
| You have a bounded bug, feature, or design you can reason through in one session | Run `/grill-with-docs`, then explicitly ask the agent to create or update the ticket with what you agreed.                                                                                                       |
| You expect implementation to need several sessions                               | Run `/grill-with-docs`, then `/to-spec` to publish the spec as a GitHub issue. Share it for team feedback, then run `/to-tickets` to propose a breakdown and create implementation tickets after you approve it. |
| You have a large, uncertain effort whose decisions cannot fit in one session     | Run `/wayfinder` to create the map and its decision issues. Once the route is clear, use `/to-spec` to publish the spec, get team feedback, then `/to-tickets` to create the approved implementation tickets.    |

`/grill-with-docs` updates `GLOSSARY.md` when terminology is settled and records
significant trade-offs in ADRs when warranted. It does **not** create a GitHub
issue; the explicit request in the table is a separate step.

Keep the discussion and ticket-writing steps in the same session. If an issue already
exists, update it rather than create a duplicate. Check that the tickets
capture what you agreed.

Scope each implementation ticket to fit in one fresh agent context window. If
it is too large, split it into smaller, independently verifiable pieces before
starting. Implement one ticket per fresh session for a multi-ticket build.

Wayfinder's decision issues are planning questions, not implementation tickets.
A long but well-understood build does not need Wayfinder just because it is long.
If scope expands or unresolved decisions emerge during implementation, stop and
return to planning.

### Share the thinking before the code

- **Bounded work that changes product behaviour or shared contracts:** share the
  source ticket with the proposed approach, scope, and open questions before
  implementation. A routine fix within an agreed spec does not need another
  team planning round.
- **Wayfinder work:** share the map early, once the destination, scope, and first
  decision questions are visible. Ask whether we are solving the right problem
  and which constraints are missing. Bring consequential decisions back to the
  team as the map develops; do not wait until every decision is settled.
- **Multi-session builds:** share the resulting spec before splitting it into
  implementation tickets. Ask for feedback on behaviour, boundaries, trade-offs,
  and what is out of scope. Share the ticket breakdown when dependencies or
  ownership need coordination.

Share widely, but always @mention the relevant people you want feedback from in Slack. Link the artifact and say what feedback you need, your recommendation,
and what is still undecided. Resolve feedback that affects the approach before building,
and record the outcome in the ticket or spec. A chat transcript is not the
handoff, and a PR should not be the team's first sight of a significant decision.

[Plannotator](https://plannotator.ai/) can help you respond to agent Q&A:
annotate the questions and recommendations with your answers or pushback, then
return that feedback to the agent.

## 3. Implement the agreed scope

Work on a branch for your PR. Pass the agreed issue and spec context to
`/implement`. Stay involved. Challenge speculative features, unnecessary
abstractions, and local patches that hide the underlying problem. Prefer the
smallest complete fix at the right boundary, not necessarily the smallest diff.

The skill includes testing, a review, and a commit. That review does **not**
replace the fresh-session review below. Follow the repository's validation
requirements rather than treating the skill's defaults as sufficient.

## 4. Review the implementation

**Review the code yourself.** Agent review does not replace reading and
understanding the implementation. You may only see that the approach is wrong
when you look at the code. Recognising that is your responsibility. [Plannotator's code review tool](https://docs.plannotator.ai/code-review)
can help you explore and annotate the diff.

Run `/code-review` in a **new session**, and use that time to review the code
yourself in parallel. Give the review agent a prompt such as:

```text
/code-review since main
Review against <source issue URL>.
Additional context, if relevant: <spec, architecture decision, or map links>.
Read relevant comments and amendments. Check both repo standards and spec fit.
Flag scope creep. Do not invent requirements for hypothetical future needs.
```

**Agent findings are input to your judgement, not a new backlog.** For each
finding, ask:

- Is it supported by the code and agreed requirements?
- Does it identify a real correctness, security, or maintainability problem?
- Is the proposed fix necessary now, and does it address the cause?
- Does it add behaviour or flexibility nobody has asked for?

Bias towards **YAGNI: you aren't gonna need it**. Push back on speculative
features and future-proofing. This is not an excuse to dismiss real bugs or skip
required safeguards.

Take the findings back to the implementation session with your decisions:
what to fix, what to reject, and why. Do not paste the report with “fix everything.”
Verify the fixes and re-review materially changed code as needed; do not chase
an endless cycle of agent-generated improvements.

### Decide whether to iterate or restart

Implementation teaches us things that planning did not. A poor approach often
means we left the work underspecified, with too many open decisions for the
agent to make itself.

An oversized ticket can also be the problem. Before retrying, consider splitting
it rather than asking another agent to tackle the same scope.

Use your judgement: can focused changes fix the approach, or should you discard
this implementation and start again? Implementation is cheap; existing code is
not a reason to keep the wrong design.

If you restart, explore the change with the implementation agent first. Identify
what you learned and which decisions need to be settled. Ask it to write a
`/handoff` for the next agent, capturing those lessons and linking existing
artifacts. Update the ticket if the learning changes or clarifies the agreed
work. Then start a fresh implementation session with the ticket and handoff.

## 5. Ship work you can stand behind

### Open a PR with a useful description

Write for future code archaeology. Someone returning to this change needs to
understand **why** it happened. Link the source ticket
for the problem, context, and agreed scope. Capture any new rationale there
rather than burying it in a generic agent-generated summary or file-by-file list.

Try running `/show-me` on the change and adding its output to the PR description:
a focused diagram, before-and-after sketch, or control flow can explain more
than a wall of prose. Check that it reflects the final implementation. Paste
Markdown-compatible output directly, or attach a screenshot or accessible link
for an HTML visual.

For UI features, attach a screen recording of the interaction so future readers
can see how the feature behaved.

### Assess feedback and merge

While Cubic's automated PR reviews are enabled, apply the same judgement as
for `/code-review`: assess each finding, address real problems, and push back
on speculative scope or unnecessary changes. Do not treat its findings as a
mandatory backlog. Cubic does not replace your own review or the fresh-session
review. Whether we keep Cubic enabled is still undecided.

If you want a second pair of eyes, assign the PR to the relevant teammates and
say what you want them to look at.

When the work is ready, you can merge the PR yourself.

## Ask for help, not a substitute owner

Ask teammates for input whenever it helps: an unclear requirement, a design
trade-off, an unfamiliar area, a disputed finding, or a second pair of human
eyes. You do not have to solve everything alone.

But asking someone else to ask their agent to review your agent's work is an
inefficient default. Run that review yourself and assess it first. Bring a
specific question, the relevant context, and your current judgement when you
need help.

---

This workflow uses the installed [AI Hero / Matt Pocock skills](https://github.com/mattpocock/skills).
See [the skill flow guide](../.agents/skills/ask-matt/SKILL.md) for the wider toolkit.
