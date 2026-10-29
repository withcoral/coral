/*
 * Stored discovery responses for stories and tests, in the shape Coral retains
 * as the `response_json` of a `search` or `describe` attempt.
 *
 * Captured from a local Coral server on 2026-10-02, before Coral trimmed
 * Search Hits:
 * - `untrimmed`: the page for the query "github pulls", limit 5, no provider filter.
 * - `describeTool`: the describe answer for `issues.create`.
 * - `describeFunction`: the describe answer for a function saved for the
 *   purpose. Its diagnostic is real.
 *
 * Built from those captures, to show each branch:
 * - `complete`: the captured page as Coral writes it now.
 * - `mixed`: a Saved Function hit, a guidance match, a matched input, and flags.
 * - `partial` and `error`: the same hits with incomplete Search Coverage.
 * - `sparse`: a Saved Function hit, then a hit whose signature and description are empty.
 * - `earlierShape`: the response before Search Hits. The parser rejects it.
 */
const githubHits = [
  {
    kind: 'provider_tool',
    call_path: 'coral.providers.github.pulls.checkIfMerged',
    signature_line:
      'checkIfMerged(input: { owner: string; pull_number: number; repo: string }): Promise<{ data: null }>',
    description_snippet:
      'Checks if a pull request has been merged into the base branch. The HTTP status of the response indicates whether or not the pull request has been merged; the response body is empty.',
    evidence: {
      fields: ['call_path', 'provider_display_name', 'provider_namespace', 'tags'],
      paths: [],
    },
    provider_display_name: 'GitHub',
    title: 'Check if a pull request has been merged',
    hints: {},
    approval_required: false,
    deprecated: false,
    matched_tags: ['pulls'],
  },
  {
    kind: 'provider_tool',
    call_path: 'coral.providers.github.pulls.deleteReviewComment',
    signature_line:
      'deleteReviewComment(input: { comment_id: number; owner: string; repo: string }): Promise<{ data: null }>',
    description_snippet: 'Deletes a review comment.',
    evidence: {
      fields: ['call_path', 'provider_display_name', 'provider_namespace', 'tags'],
      paths: [],
    },
    provider_display_name: 'GitHub',
    title: 'Delete a review comment for a pull request',
    hints: {},
    approval_required: false,
    deprecated: false,
    matched_tags: ['pulls'],
  },
  {
    kind: 'provider_tool',
    call_path: 'coral.providers.github.pulls.merge',
    signature_line:
      'merge(input: { body?: PutReposOwnerRepoPullsPull_numberMergeBodyInput; owner: string; pull_number: number; repo: string }): Promise<{ data: PullRequestMergeResult }>',
    description_snippet:
      '> [!NOTE]\n> We recommend using the [asynchronous merge API](https://docs.github.com/rest/pulls/pulls#merge-a-pull-request-asynchronously) instead. This endpoint does not support stacked pull requests or merging with a merge queue.\n\nMerges a pull request into the base branch.\n\nThis endpoint triggers [notifications](https://docs.github.com/github/managing-subscriptions-and-notifications-on-github/about-notifications).\nCreating content too quickly using this endpoint may result in secondary rate…',
    evidence: {
      fields: ['call_path', 'description', 'provider_display_name', 'provider_namespace', 'tags'],
      paths: [],
    },
    provider_display_name: 'GitHub',
    title: 'Merge a pull request',
    hints: {},
    approval_required: false,
    deprecated: false,
    matched_tags: ['pulls'],
  },
  {
    kind: 'provider_tool',
    call_path: 'coral.providers.github.pulls.getMergeAsyncResult',
    signature_line:
      'getMergeAsyncResult(input: { owner: string; pull_number: number; repo: string; uuid: string }): Promise<{ data: PullRequestMergeAsyncResult }>',
    description_snippet:
      '…result does not change when the merge queue later merges the pull request. To get the eventual merge status, [check if a pull request has been merged](https://docs.github.com/rest/pulls/pulls#check-if-a-pull-request-has-been-merged).\n\nThe result of an asynchronous merge request is retained for 24 hours after its most recent update. After this window the request expires and this endpoint returns a `404` response for its UUID.',
    evidence: {
      fields: ['call_path', 'description', 'provider_display_name', 'provider_namespace', 'tags'],
      paths: [],
    },
    provider_display_name: 'GitHub',
    title: 'Get the result of an asynchronous merge',
    hints: {},
    approval_required: false,
    deprecated: false,
    matched_tags: ['pulls'],
  },
  {
    kind: 'provider_tool',
    call_path: 'coral.providers.github.pulls.updateBranch',
    signature_line:
      'updateBranch(input: { body?: PutReposOwnerRepoPullsPull_numberUpdateBranchBodyInput; owner: string; pull_number: number; repo: string }): Promise<{ data: {…} }>',
    description_snippet:
      'Updates the pull request branch with the latest upstream changes by merging HEAD from the base branch into the pull request branch.\nNote: If making a request on behalf of a GitHub App you must also have permissions to write the contents of the head repository.',
    evidence: {
      fields: ['call_path', 'description', 'provider_display_name', 'provider_namespace', 'tags'],
      paths: ['input.body.expected_head_sha'],
    },
    provider_display_name: 'GitHub',
    title: 'Update a pull request branch',
    hints: {},
    approval_required: false,
    deprecated: false,
    matched_tags: ['pulls'],
  },
]

/** Real hit from the query "github issue get sub-issues list"; paths cut to a third. */
const subIssuesHit = {
  kind: 'provider_tool',
  call_path: 'coral.providers.github.issues.listSubIssues',
  signature_line:
    'listSubIssues(input: { issue_number: number; owner: string; page?: number; per_page?: number; repo: string }): Promise<{ data: Array<Issue> }>',
  description_snippet:
    'You can use the REST API to list the sub-issues on an issue.\n\nThis endpoint supports the following custom media types. For more information, see [Media types](https://docs.github.com/rest/using-the-rest-api/getting-started-with-the-rest-api#media-types).\n\n- **`application/vnd.github.raw+json`**: Returns the raw Markdown body. Response will include `body`. This is the default if you do not pass any specific media type.\n- **`application/vnd.github.text+json`**: Returns a text only representation of the…',
  evidence: {
    fields: [
      'call_path',
      'description',
      'provider_display_name',
      'provider_namespace',
      'tags',
      'title',
    ],
    paths: [
      'input.issue_number',
      'result.data[]',
      'result.data[].assignee',
      'result.data[].assignee.subscriptions_url',
      'result.data[].assignees[]',
      'result.data[].body',
      'result.data[].closed_by',
      'result.data[].issue_dependencies_summary',
      'result.data[].issue_dependencies_summary.blocked_by',
      'result.data[].issue_dependencies_summary.blocking',
      'result.data[].issue_field_values[]',
      'result.data[].issue_field_values[].data_type',
    ],
  },
  provider_display_name: 'GitHub',
  title: 'List sub-issues',
  hints: {},
  approval_required: false,
  deprecated: false,
  matched_tags: ['issues'],
}

const functionHit = {
  kind: 'saved_function',
  call_path: 'coral.functions.github.openIssuesByLabel',
  signature_line:
    'openIssuesByLabel(input: { owner: string; repo: string; label: string; limit?: number }): Promise<Array<{ number: number; title: string; url: string }>>',
  description_snippet:
    'Lists the open issues of one repository that carry a label, newest first, with number, title and URL. Use it before triage to see what waits under a label.',
  evidence: { fields: ['call_path', 'description'], paths: ['input.label'] },
}

interface UntrimmedHit {
  evidence: { fields: string[]; paths: string[] }
  hints?: Record<string, boolean>
  approval_required?: boolean
  deprecated?: boolean
  [key: string]: unknown
}
/**
 * One captured hit as Coral writes it now. It drops what the Call Path, the
 * rank, or describe states, and the flags and hints that are not set.
 */
function trimmed({
  kind: _kind,
  title: _title,
  provider_display_name: _provider,
  evidence: _evidence,
  hints = {},
  approval_required,
  deprecated,
  ...kept
}: UntrimmedHit) {
  return {
    ...kept,
    ...(Object.keys(hints).length > 0 && { hints }),
    ...(approval_required && { approval_required }),
    ...(deprecated && { deprecated }),
  }
}

const untrimmed = {
  hits: githubHits,
  next_cursor:
    'eyJ2ZXJzaW9uIjoxLCJwb3NpdGlvbiI6NSwiZGlnZXN0IjoiNWMyMDc2MWY2MzJkZjYyZWI2NzY0MDNiMjY3Y2Q4NGMxZTE2NzNlM2Y0MTMyMDI2Y2M0N2ZhNDRiOTcyYWE3NiJ9',
  has_more: true,
  retriever_statuses: [
    {
      retriever: 'saved_function',
      state: 'empty',
      coverage: {
        eligible_units: 0,
        searched_units: 0,
        failed_units: 0,
        returned_count: 0,
        has_more: false,
      },
    },
    {
      retriever: 'provider_tool',
      state: 'results_found',
      coverage: {
        eligible_units: 1,
        searched_units: 1,
        failed_units: 0,
        returned_count: 100,
        has_more: true,
      },
    },
  ],
}

/** Every retriever searched the captured page, so Coral states no coverage. */
const complete = {
  hits: githubHits.map(trimmed),
  next_cursor: untrimmed.next_cursor,
  has_more: true,
}

const mixed = {
  hits: [
    functionHit,
    {
      ...githubHits[2],
      guidance_snippet: 'API method documentation',
      approval_required: true,
      hints: { destructive: true, idempotent: false },
    },
    { ...subIssuesHit, matched_inputs: ['per_page'] },
    { ...githubHits[0], deprecated: true },
    githubHits[3],
    githubHits[4],
  ].map(trimmed),
  next_cursor: untrimmed.next_cursor,
  has_more: true,
}

const sparse = {
  hits: [
    trimmed(functionHit),
    { call_path: 'coral.providers.github.pulls.get', signature_line: '', description_snippet: '' },
  ],
  has_more: false,
}

const partial = {
  hits: githubHits.slice(0, 3).map(trimmed),
  has_more: false,
  retriever_statuses: [
    {
      retriever: 'provider_tool',
      state: 'partial',
      coverage: {
        eligible_units: 3,
        searched_units: 2,
        failed_units: 1,
        returned_count: 3,
        has_more: false,
      },
    },
  ],
}

const error = {
  hits: [trimmed(functionHit)],
  has_more: false,
  retriever_statuses: [
    {
      retriever: 'provider_tool',
      state: 'error',
      coverage: {
        eligible_units: 3,
        searched_units: 0,
        failed_units: 3,
        returned_count: 0,
        has_more: false,
      },
    },
  ],
}

const HTTP_METADATA =
  'metadata: { readonly kind: "http"; readonly status: number; readonly headers: readonly (readonly [string, string])[]; readonly mediaType: string | null; readonly charset: string | null; readonly finalUrl: string; readonly redirects: readonly { readonly status: number; readonly url: string; readonly crossOrigin: boolean }[]; readonly decodeDiagnostics: readonly { readonly code: string; readonly message: string }[] }'

const describeTool = {
  kind: 'provider_tool',
  call_path: 'coral.providers.github.issues.create',
  description:
    'Any user with pull access to a repository can create an issue. If [issues are disabled in the repository](https://docs.github.com/articles/disabling-issues/), the API returns a `410 Gone` status.\n\nThis endpoint triggers [notifications](https://docs.github.com/github/managing-subscriptions-and-notifications-on-github/about-notifications). Creating content too quickly using this endpoint may result in secondary rate limiting. For more information, see "[Rate limits for the API](https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api#about-secondary-rate-limits)"\nand "[Best practices for using the REST API](https://docs.github.com/rest/guides/best-practices-for-using-the-rest-api)."\n\nThis endpoint supports the following custom media types. For more information, see "[Media types](https://docs.github.com/rest/using-the-rest-api/getting-started-with-the-rest-api#media-types)."\n\n- **`application/vnd.github.raw+json`**: Returns the raw markdown body. Response will include `body`. This is the default if you do not pass any specific media type.\n- **`application/vnd.github.text+json`**: Returns a text only representation of the markdown body. Response will include `body_text`.\n- **`application/vnd.github.html+json`**: Returns HTML rendered from the body\'s markdown. Response will include `body_html`.\n- **`application/vnd.github.full+json`**: Returns raw, text, and HTML representations. Response will include `body`, `body_text`, and `body_html`.',
  typescript_declaration: `type AuthorAssociation = "COLLABORATOR" | "CONTRIBUTOR" | "FIRST_TIMER" | "FIRST_TIME_CONTRIBUTOR" | "MANNEQUIN" | "MEMBER" | "NONE" | "OWNER";\ntype BasicError = ({ "documentation_url"?: string; "message"?: string; "status"?: string; "url"?: string } & Record<string, unknown>);\ntype Enterprise = ({ "avatar_url": string; "created_at": string | null; "description"?: string | null; "html_url": string; "id": number; "name": string; "node_id": string; "slug": string; "updated_at": string | null; "website_url"?: string | null } & Record<string, unknown>);\ntype Issue = ({ "active_lock_reason"?: string | null; "assignee": NullableSimpleUser; "assignees"?: Array<SimpleUser>; "author_association"?: AuthorAssociation; "body"?: string | null; "body_html"?: string; "body_text"?: string; "closed_at": string | null; "closed_by"?: NullableSimpleUser; "comments": number; "comments_url": string; "created_at": string; "draft"?: boolean; "events_url": string; "html_url": string; "id": number; "issue_dependencies_summary"?: IssueDependenciesSummary; "issue_field_values"?: Array<IssueFieldValue>; "labels": Array<(string | ({ "archived_by"?: (SimpleUser); "color"?: string | null; "default"?: boolean; "description"?: string | null; "id"?: number; "name"?: string; "node_id"?: string; "url"?: string } & Record<string, unknown>))>; "labels_url": string; "locked": boolean; "milestone": NullableMilestone; "node_id": string; "number": number; "parent_issue_url"?: string | null; "performed_via_github_app"?: NullableIntegration; "pinned_comment"?: NullableIssueComment; "pull_request"?: ({ "diff_url": string | null; "html_url": string | null; "merged_at"?: string | null; "patch_url": string | null; "url": string | null } & Record<string, unknown>); "reactions"?: ReactionRollup; "repository"?: Repository; "repository_url": string; "state": string; "state_reason"?: "completed" | "reopened" | "not_planned" | "duplicate" | null; "sub_issues_summary"?: SubIssuesSummary; "timeline_url"?: string; "title": string; "type"?: IssueType; "updated_at": string; "url": string; "user": NullableSimpleUser } & Record<string, unknown>);\ntype IssueDependenciesSummary = ({ "blocked_by": number; "blocking": number; "total_blocked_by": number; "total_blocking": number } & Record<string, unknown>);\ntype IssueFieldValue = ({ "data_type": "text" | "single_select" | "multi_select" | "number" | "date"; "issue_field_id": number; "issue_field_name"?: string; "multi_select_options"?: Array<({ "color": string; "id": number; "name": string } & Record<string, unknown>)> | null; "node_id": string; "single_select_option"?: ({ "color": string; "id": number; "name": string } & Record<string, unknown>) | null; "value": (string | number | number) } & Record<string, unknown>);\ntype IssueType = ({ "color"?: "gray" | "blue" | "green" | "yellow" | "orange" | "red" | "pink" | "purple" | null; "created_at"?: string; "description": string | null; "id": number; "is_enabled"?: boolean; "name": string; "node_id": string; "updated_at"?: string } & Record<string, unknown>) | null;\ntype NullableIntegration = ({ "client_id"?: string; "created_at": string; "description": string | null; "events": Array<string>; "external_url": string; "html_url": string; "id": number; "installations_count"?: number; "name": string; "node_id": string; "owner": (SimpleUser | Enterprise); "permissions": ({ "checks"?: string; "contents"?: string; "deployments"?: string; "issues"?: string; "metadata"?: string } & Record<string, unknown>); "slug"?: string; "updated_at": string } & Record<string, unknown>) | null;\ntype NullableIssueComment = ({ "author_association"?: AuthorAssociation; "body"?: string; "body_html"?: string; "body_text"?: string; "created_at": string; "html_url": string; "id": number; "issue_url": string; "minimized"?: NullableIssueCommentMinimized; "node_id": string; "performed_via_github_app"?: NullableIntegration; "pin"?: NullablePinnedIssueComment; "reactions"?: ReactionRollup; "updated_at": string; "url": string; "user": NullableSimpleUser } & Record<string, unknown>) | null;\ntype NullableIssueCommentMinimized = ({ "reason": string | null } & Record<string, unknown>) | null;\ntype NullableLicenseSimple = ({ "html_url"?: string; "key": string; "name": string; "node_id": string; "spdx_id": string | null; "url": string | null } & Record<string, unknown>) | null;\ntype NullableMilestone = ({ "closed_at": string | null; "closed_issues": number; "created_at": string; "creator": NullableSimpleUser; "description": string | null; "due_on": string | null; "html_url": string; "id": number; "labels_url": string; "node_id": string; "number": number; "open_issues": number; "state": "open" | "closed"; "title": string; "updated_at": string; "url": string } & Record<string, unknown>) | null;\ntype NullablePinnedIssueComment = ({ "pinned_at": string; "pinned_by": NullableSimpleUser } & Record<string, unknown>) | null;\ntype NullableSimpleUser = ({ "avatar_url": string; "email"?: string | null; "events_url": string; "followers_url": string; "following_url": string; "gists_url": string; "gravatar_id": string | null; "html_url": string; "id": number; "login": string; "name"?: string | null; "node_id": string; "organizations_url": string; "received_events_url": string; "repos_url": string; "site_admin": boolean; "starred_at"?: string; "starred_url": string; "subscriptions_url": string; "type": string; "url": string; "user_view_type"?: string } & Record<string, unknown>) | null;\ntype OwnerParameterInput = string;\ntype PostReposOwnerRepoIssuesBodyInput = ({ "assignee"?: string | null; "assignees"?: Array<string>; "body"?: string; "issue_field_values"?: Array<{ "field_id": number; "value": (string | number | Array<string>) }>; "labels"?: Array<(string | ({ "color"?: string | null; "description"?: string | null; "id"?: number; "name"?: string } & Record<string, unknown>))>; "milestone"?: (string | number); "parent_issue_id"?: number; "title": (string | number); "type"?: string | null } & Record<string, unknown>);\ntype ReactionRollup = ({ "+1": number; "-1": number; "confused": number; "eyes": number; "heart": number; "hooray": number; "laugh": number; "rocket": number; "total_count": number; "url": string } & Record<string, unknown>);\ntype RepoParameterInput = string;\ntype Repository = ({ "allow_auto_merge"?: boolean; "allow_forking"?: boolean; "archive_url": string; "archived": boolean; "default_branch": string; "description": string | null; "disabled": boolean; "fork": boolean; "forks_count": number; "full_name": string; "html_url": string; "id": number; "language": string | null; "license": NullableLicenseSimple; "name": string; "node_id": string; "open_issues_count": number; "owner": SimpleUser; "private": boolean; "pushed_at": string | null; "size": number; "stargazers_count": number; "topics"?: Array<string>; "updated_at": string | null; "url": string; "visibility"?: string; "watchers_count": number } & Record<string, unknown>);\ntype ScimError = ({ "detail"?: string | null; "documentation_url"?: string | null; "message"?: string | null; "schemas"?: Array<string>; "scimType"?: string | null; "status"?: number } & Record<string, unknown>);\ntype SimpleUser = ({ "avatar_url": string; "email"?: string | null; "events_url": string; "followers_url": string; "following_url": string; "gists_url": string; "gravatar_id": string | null; "html_url": string; "id": number; "login": string; "name"?: string | null; "node_id": string; "organizations_url": string; "received_events_url": string; "repos_url": string; "site_admin": boolean; "starred_at"?: string; "starred_url": string; "subscriptions_url": string; "type": string; "url": string; "user_view_type"?: string } & Record<string, unknown>);\ntype SubIssuesSummary = ({ "completed": number; "percent_completed": number; "total": number } & Record<string, unknown>);\ntype ValidationError = ({ "documentation_url": string; "errors"?: Array<({ "code": string; "field"?: string; "index"?: number; "message"?: string; "resource"?: string; "value"?: (string | null | number | null | Array<string> | null) } & Record<string, unknown>)>; "message": string } & Record<string, unknown>);\nnamespace issues { function create(input: { "body": PostReposOwnerRepoIssuesBodyInput; "owner": OwnerParameterInput; "repo": RepoParameterInput }): Promise<{ data: ({ "code"?: string; "documentation_url"?: string; "message"?: string } & Record<string, unknown>); ${HTTP_METADATA} & { readonly status: 503; readonly mediaType: "application/json"; readonly decodeDiagnostics: readonly [] } } | { data: BasicError; ${HTTP_METADATA} & { readonly status: 400; readonly mediaType: "application/json"; readonly decodeDiagnostics: readonly [] } } | { data: BasicError; ${HTTP_METADATA} & { readonly status: 403; readonly mediaType: "application/json"; readonly decodeDiagnostics: readonly [] } } | { data: BasicError; ${HTTP_METADATA} & { readonly status: 404; readonly mediaType: "application/json"; readonly decodeDiagnostics: readonly [] } } | { data: BasicError; ${HTTP_METADATA} & { readonly status: 410; readonly mediaType: "application/json"; readonly decodeDiagnostics: readonly [] } } | { data: Issue; ${HTTP_METADATA} & { readonly status: 201; readonly mediaType: "application/json"; readonly decodeDiagnostics: readonly [] } } | { data: ScimError; ${HTTP_METADATA} & { readonly status: 400; readonly mediaType: "application/scim+json"; readonly decodeDiagnostics: readonly [] } } | { data: ValidationError; ${HTTP_METADATA} & { readonly status: 422; readonly mediaType: "application/json"; readonly decodeDiagnostics: readonly [] } } | { data: unknown; ${HTTP_METADATA} & { readonly decodeDiagnostics: readonly [{ readonly code: string; readonly message: string }, ...{ readonly code: string; readonly message: string }[]] } }>; }`,
  provider_access: 'github',
  provider_id: '67574d58-bd6f-4f55-a316-c4edf40ea188',
  provider_display_name: 'GitHub',
  title: 'Create an issue',
  deprecated: false,
  tags: ['issues'],
  guidance: 'API method documentation',
  hints: {},
  approval_required: false,
}

const describeFunction = {
  kind: 'saved_function',
  call_path: 'coral.functions.github.openIssuesByLabel',
  description:
    'Lists the open issues of one repository that carry a label, newest first, with number, title and URL. Use it before triage to see what waits under a label.',
  typescript_declaration:
    'export default function openIssuesByLabel(input: {\n    owner: string;\n    repo: string;\n    label: string;\n    limit?: number;\n}): Promise<Array<{\n    number: number;\n    title: string;\n    url: string;\n}>>;\n',
  source:
    "export default async function openIssuesByLabel(input: {\n  owner: string\n  repo: string\n  label: string\n  limit?: number\n}): Promise<Array<{ number: number; title: string; url: string }>> {\n  const limit = input.limit ?? 20\n  const page = await coral.providers.github.issues.listForRepo({\n    owner: input.owner,\n    repo: input.repo,\n    labels: input.label,\n    state: 'open',\n    sort: 'created',\n    direction: 'desc',\n    per_page: limit,\n  })\n  return (page.data as any[])\n    .filter((issue) => !issue.pull_request)\n    .map((issue) => ({ number: issue.number, title: issue.title, url: issue.html_url }))\n}\n",
  diagnostics: [
    { category: 'error', code: 2304, message: "Cannot find name 'coral'.", line: 8, column: 22 },
  ],
  diagnostics_truncated: false,
}

/** Each response as the stored JSON text. */
export const stored = {
  complete: JSON.stringify(complete),
  untrimmed: JSON.stringify(untrimmed),
  mixed: JSON.stringify(mixed),
  sparse: JSON.stringify(sparse),
  partial: JSON.stringify(partial),
  error: JSON.stringify(error),
  describeTool: JSON.stringify(describeTool),
  describeFunction: JSON.stringify(describeFunction),
  earlierShape: JSON.stringify({
    items: [{ call_path: 'coral.providers.github.pulls.merge', typescript_declaration: '…' }],
    next_cursor: null,
  }),
}
