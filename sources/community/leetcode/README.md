# LeetCode Source for Coral

LeetCode has become one of the most widely used platforms for learning algorithms, preparing for technical interviews, and improving problem-solving skills. Whether you're a student learning data structures for the first time, an engineer preparing for interviews, or someone who simply enjoys solving programming challenges, there is a large amount of valuable information contained within the LeetCode problem catalog.

This source brings that information into Coral.

Instead of navigating through the LeetCode website manually, you can query problems using SQL. This makes it easier to search, filter, analyze, and integrate LeetCode data into your own workflows, dashboards, research projects, AI agents, or educational tools.

The source is built on top of LeetCode's public GraphQL API and exposes problem metadata as a table while exposing detailed problem information through a table function.

With this source, you can:

- Browse thousands of programming problems.
- Filter by difficulty level.
- Discover problems by topic tags.
- Analyze acceptance rates.
- Retrieve full problem statements.
- Access hints and starter code templates.
- Explore similar problems for continued practice.
- Build custom learning and interview-preparation workflows using SQL.

---

## Data Model

The source is intentionally divided into two parts.

### 1. Problem Discovery

When exploring problems, you usually want lightweight information:

- Problem number
- Title
- Difficulty
- Acceptance rate
- Topic tags

This information is available through the `leetcode.problems` table.

Think of this table as a searchable catalog of the LeetCode problem set.

### 2. Problem Details

Once you find an interesting problem, you often want additional information:

- Full problem statement
- Hints
- Sample test cases
- Similar questions
- Starter code snippets

Fetching all of this information for every problem would be expensive and unnecessary.

Instead, detailed information is exposed through the `problem_detail` table function, allowing you to request details only when needed.

This design keeps exploration fast while still providing access to rich problem content.

---

## Tables and Functions

### `leetcode.problems`

Lists problems from the LeetCode problem catalog.

This table is designed for discovery, filtering, and analysis.

Typical use cases include:

- Finding Easy problems for beginners.
- Exploring Dynamic Programming questions.
- Building interview preparation plans.
- Studying acceptance-rate trends.
- Discovering problems related to a particular topic.

#### Columns

| Column | Type | Description |
|---|---|---|
| `frontend_id` | `Utf8` | Human-readable problem number shown on LeetCode |
| `title` | `Utf8` | Problem title |
| `title_slug` | `Utf8` | URL slug used by LeetCode |
| `difficulty` | `Utf8` | Problem difficulty |
| `ac_rate` | `Float64` | Acceptance rate |
| `is_paid_only` | `Boolean` | Premium-only indicator |
| `topic_tags` | `Json` | Topic tags associated with the problem |
| `status` | `Utf8` | Submission status when available |

#### Supported Filters

| Filter | Description |
|---|---|
| `difficulty` | Filter by difficulty level |
| `title_slug` | Retrieve a specific problem |

---

#### Example: Browse Problems

```sql
SELECT frontend_id, title, difficulty
FROM leetcode.problems
LIMIT 20
```

This query provides a quick overview of available problems in the LeetCode catalog.

#### Example: Find Easy Problems

```sql
SELECT frontend_id, title, ac_rate
FROM leetcode.problems
WHERE difficulty = 'Easy'
ORDER BY ac_rate DESC
LIMIT 20
```

A useful query for beginners who want approachable problems with relatively high success rates.

#### Example: Explore Dynamic Programming

```sql
SELECT frontend_id, title, difficulty
FROM leetcode.problems
WHERE json_contains(topic_tags, 'Dynamic Programming')
LIMIT 30
```

This query helps identify problems focused on Dynamic Programming concepts.

#### Example: Interview Preparation

```sql
SELECT frontend_id, title, ac_rate
FROM leetcode.problems
WHERE difficulty = 'Medium'
  AND json_contains(topic_tags, 'Array')
ORDER BY ac_rate DESC
LIMIT 20
```

A practical query for candidates preparing for coding interviews.

---

### `leetcode.problem_detail(title_slug => '...')`

Returns detailed information for a specific problem.

This function is intended for deeper study after a problem has been discovered through the `problems` table.

#### Columns

| Column | Type | Description |
|---|---|---|
| `frontend_id` | `Utf8` | Problem number |
| `title` | `Utf8` | Problem title |
| `content` | `Utf8` | Full problem statement |
| `difficulty` | `Utf8` | Problem difficulty |
| `likes` | `Int64` | Community upvotes |
| `dislikes` | `Int64` | Community downvotes |
| `hints` | `Json` | Available hints |
| `sample_test_case` | `Utf8` | Sample input |
| `ac_rate` | `Float64` | Acceptance rate |
| `stats` | `Json` | Submission statistics |
| `similar_questions` | `Json` | Related problems |
| `topic_tags` | `Json` | Problem tags |
| `code_snippets` | `Json` | Starter code templates |

#### Example: Retrieve a Full Problem Statement

```sql
SELECT title, difficulty, content
FROM leetcode.problem_detail(
  title_slug => 'two-sum'
)
```

This returns the complete problem description for the Two Sum problem.

#### Example: Retrieve Hints and Starter Code

```sql
SELECT title, hints, code_snippets
FROM leetcode.problem_detail(
  title_slug => 'longest-substring-without-repeating-characters'
)
```

Useful when building study tools, coding assistants, or educational applications.

#### Example: View Similar Problems

```sql
SELECT title, similar_questions
FROM leetcode.problem_detail(
  title_slug => 'two-sum'
)
```

This helps discover related problems for additional practice.

#### Example: Analyze Problem Metadata

```sql
SELECT
  title,
  difficulty,
  likes,
  dislikes,
  ac_rate
FROM leetcode.problem_detail(
  title_slug => 'binary-tree-inorder-traversal'
)
```

Useful for understanding problem popularity and difficulty.

---

## Installation

```bash
coral source add --file sources/community/leetcode/manifest.yaml
```

Verify the source:

```bash
coral source test leetcode
```

No credentials are required.

---

## Notes

- Topic tags are returned as JSON and can be queried using Coral's JSON functions.
- Problem statements are returned as HTML content.
- Starter code templates are available for all languages supported by LeetCode.
- Paid-only problems may provide limited content depending on availability.
- LeetCode may apply rate limits to excessive request volumes, so queries should remain reasonably bounded.

---

## Final Thoughts

This source was created to make LeetCode data easier to explore, analyze, and integrate into custom workflows.

Whether you're building interview-preparation tools, educational applications, AI assistants, personal dashboards, or simply exploring programming challenges through SQL, this source provides a convenient bridge between Coral and the LeetCode problem ecosystem.

If you discover an issue or have an idea for improvement, contributions and feedback are always welcome.