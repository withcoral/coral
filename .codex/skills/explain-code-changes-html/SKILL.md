---
name: explain-code-changes-html
description: Create clear standalone HTML explainers for code changes, PRs, diffs, architecture updates, migrations, refactors, new APIs, config changes, or implementation handoffs. Use when Codex is asked to make code changes understandable with an HTML document, visual walkthrough, architecture map, sequence/data-flow diagram, reviewer guide, or junior-friendly team explanation.
---

# Explain Code Changes With HTML

Create one self-contained HTML reference document that helps a mixed-seniority
engineering team understand a code change quickly and accurately.

## Workflow

1. Inspect the change before writing.
   - Read the diff against the requested target branch, or infer the target
     branch from the repo when the user does not specify one.
   - Identify the motivation, behavior changes, files touched, runtime flow,
     data contracts, tests, risks, and non-goals.
   - Trace every claim to the diff, tests, or surrounding repo context.
   - Mark uncertainty as an assumption. Omit claims that cannot be supported.

2. Choose the smallest useful presentation format.
   - Default to a single standalone `.html` file with semantic HTML,
     responsive CSS, and inline SVG.
   - Use Mermaid only when diagrams-as-code materially improves maintainability
     for flowcharts, sequence diagrams, state diagrams, ER diagrams, git graphs,
     or dependency maps.
   - Use Reveal.js only when the user asks for a slide deck or live
     presentation.
   - Use Shiki or Prism only when syntax highlighting improves comprehension.
   - Use Chart.js only for simple quantitative charts.
   - Use D3 or Observable Plot only for rich custom visualizations that cannot
     be explained faster with a static diagram, table, or simple chart.

3. Write the file where the user requested it.
   - If the user does not specify a path, create it under `.context/`.
   - Keep the output durable: no build step, no hidden local dependencies, and
     no external assets unless the chosen library is clearly justified.

4. Verify the document.
   - Open or render the HTML when practical.
   - Check laptop and mobile widths for readable text, non-overlapping layout,
     legible diagrams, and useful visual hierarchy.
   - Mention any checks that could not be run.

## Required Document Structure

Use progressive disclosure: orient first, then go deeper.

1. Title and Audience
   - Name the change plainly.
   - State who the document is for and what they should understand after
     reading.

2. Executive Summary
   - Include 3 to 5 bullets or cards.
   - Cover what changed, why it changed, and the main operational impact.

3. Mental Model
   - Explain the system in simple terms before showing details.
   - Include a small glossary for domain-specific terms.

4. Before / After
   - Show the architecture or workflow before and after.
   - Prefer diagrams over prose-only explanations.

5. Runtime or Data Flow
   - Walk step by step from input to output.
   - Show exactly where the new code plugs in.

6. Code Map
   - List important files or modules.
   - For each one, explain its responsibility, what changed, and why it matters.
   - Avoid dumping large diffs.

7. Contracts and Examples
   - Cover any API, config, data format, CLI, schema, event, or wire contract.
   - Include one realistic example and explain each important field.

8. Testing and Verification
   - List tests added or updated.
   - Include commands run and their results.
   - Explain what the tests prove and what remains unproven.

9. Risks, Boundaries, and Non-Goals
   - Call out compatibility, rollout, performance, failure modes, and operational
     risks.
   - Say "not covered" or "not changed" where that prevents over-reading.

10. FAQ / Review Guide
    - Answer likely reviewer questions.
    - Include "Where should I look first in code?"

## Design Requirements

- Use accessible text sizes, high contrast, and responsive layout.
- Use headings, cards, callouts, tables, and diagrams to reduce memory load.
- Do not rely on color alone; pair color with labels or shape.
- Do not use tiny diagram text.
- Add captions under diagrams explaining what to notice.
- Keep snippets short and focused; highlight changed lines only when helpful.
- Prefer concrete file names, module names, and runtime nouns.
- Exclude decorative visuals that do not explain the change.

## Useful Components

- Summary cards: `What changed`, `Why`, `Impact`, `Risk`.
- Architecture diagram: modules as boxes, calls or data movement as arrows.
- Sequence diagram: request or runtime lifecycle.
- File responsibility table: path, responsibility, change, reviewer note.
- Contract table: field, meaning, default, validation.
- Test matrix: scenario, test name, expected result.
- Boundary callouts: `This does not do X`.
- FAQ: common reviewer objections with concise answers.

## Final Response

After creating the HTML file, return:

- The full path to the HTML file.
- A short summary of what the document covers.
- Any external libraries used and why.
- Checks run, plus anything that could not be verified.
