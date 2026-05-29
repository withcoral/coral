import { create } from '@bufbuild/protobuf'

import {
  GetTraceResponseSchema,
  ListTracesResponseSchema,
  TraceSpanSchema,
  TraceStatus,
  TraceSummarySchema,
  type GetTraceResponse,
  type ListTracesResponse,
  type TraceSpan,
  type TraceSummary,
} from '../../../src/generated/coral/v1/traces_pb'

const BASE_TIME_NANOS = 1_764_000_000_000_000_000n

type SourceName = 'github' | 'linear' | 'slack'

interface TraceFixture {
  durationMs: number
  query: string
  rows?: number
  source: SourceName
  status?: TraceStatus
}

interface SpanFixture {
  durationMs: number
  method: 'GET' | 'POST'
  path: string
  requestBody?: unknown
  requestBodyPresent?: boolean
  requestBodySize?: number
  requestBodyTruncated?: boolean
  responseBody?: unknown
  responseBodyPresent?: boolean
  responseBodySize?: number
  responseBodyTruncated?: boolean
  source: SourceName
  statusCode?: number
  table: string
}

const traceFixtures: TraceFixture[] = [
  {
    source: 'github',
    query:
      "SELECT number, title, state FROM github.pull_requests WHERE repository = 'coral' AND state = 'open' ORDER BY updated_at DESC LIMIT 25",
    durationMs: 384,
    rows: 12,
  },
  {
    source: 'slack',
    query:
      "SELECT channel_name, user_name, text FROM slack.messages WHERE channel_name = 'eng-coral' AND text ILIKE '%release%' ORDER BY ts DESC LIMIT 50",
    durationMs: 512,
    rows: 31,
  },
  {
    source: 'linear',
    query:
      "SELECT identifier, title, state_name, assignee_name FROM linear.issues WHERE team_key = 'CORAL' AND state_type != 'completed' ORDER BY updated_at DESC LIMIT 40",
    durationMs: 438,
    rows: 18,
  },
  {
    source: 'github',
    query:
      "SELECT login, merged_pull_requests FROM github.contributors WHERE repository = 'coral' ORDER BY merged_pull_requests DESC LIMIT 10",
    durationMs: 295,
    rows: 10,
  },
  {
    source: 'slack',
    query:
      "SELECT channel_name, count(*) AS messages FROM slack.messages WHERE ts > now() - interval '7 days' GROUP BY channel_name ORDER BY messages DESC",
    durationMs: 620,
    rows: 8,
  },
  {
    source: 'github',
    query:
      "SELECT workflow_name, conclusion, run_started_at FROM github.actions_runs WHERE repository = 'coral' ORDER BY run_started_at DESC LIMIT 20",
    durationMs: 466,
    rows: 20,
  },
  {
    source: 'linear',
    query:
      "SELECT identifier, title, priority_label FROM linear.issues WHERE team_key = 'CORAL' AND title ILIKE '%playwright%' ORDER BY updated_at DESC LIMIT 10",
    durationMs: 735,
    rows: 4,
  },
  {
    source: 'slack',
    query:
      "SELECT user_name, reaction, item_text FROM slack.reactions WHERE channel_name = 'eng-coral' AND reaction IN ('eyes', 'shipit') ORDER BY ts DESC LIMIT 25",
    durationMs: 341,
    rows: 16,
  },
  {
    source: 'github',
    query:
      "SELECT number, title, author_login FROM github.issues WHERE repository = 'coral' AND labels @> ARRAY['bug'] ORDER BY created_at DESC LIMIT 15",
    durationMs: 419,
    rows: 7,
  },
  {
    source: 'linear',
    query:
      "SELECT project_name, count(*) AS open_issues FROM linear.issues WHERE state_type != 'completed' GROUP BY project_name ORDER BY open_issues DESC",
    durationMs: 548,
    rows: 6,
  },
]

const selectedTraceIndex = 6

const detailSpans: SpanFixture[] = [
  {
    source: 'linear',
    table: 'issues',
    method: 'POST',
    path: '/graphql',
    durationMs: 84,
    requestBody: {
      operationName: 'IssuesSearch',
      query: `query IssuesSearch($teamKey: String!, $query: String!, $first: Int!) {
  issues(teamKey: $teamKey, query: $query, first: $first) {
    nodes {
      identifier
      title
      priorityLabel
    }
  }
}`,
      variables: { teamKey: 'CORAL', query: 'playwright', first: 10 },
    },
    responseBody: {
      data: {
        issues: {
          nodes: [
            {
              identifier: 'CORAL-128',
              title: 'Add Playwright coverage for trace stream',
              priorityLabel: 'High',
            },
            {
              identifier: 'CORAL-134',
              title: 'Record UI review flows with screencast chapters',
              priorityLabel: 'Medium',
            },
          ],
        },
      },
    },
  },
  {
    source: 'linear',
    table: 'teams',
    method: 'POST',
    path: '/graphql',
    durationMs: 57,
    requestBody: {
      operationName: 'TeamsByKey',
      query: `query TeamsByKey($key: String!) {
  teams(key: $key) {
    nodes {
      key
      name
    }
  }
}`,
      variables: { key: 'CORAL' },
    },
    responseBody: {
      data: { teams: { nodes: [{ key: 'CORAL', name: 'Coral' }] } },
      errors: [{ message: 'Partial GraphQL error for test coverage', path: ['teams'] }],
    },
  },
  {
    source: 'github',
    table: 'pull_requests',
    method: 'GET',
    path: '/repos/oxide/coral/pulls?state=open&per_page=25',
    durationMs: 113,
    responseBody: [
      {
        number: 417,
        title: 'Add MSW Playwright trace fixtures',
        user: { login: 'ludo' },
        state: 'open',
      },
      {
        number: 412,
        title: 'Tighten Coral UI trace detail layout',
        user: { login: 'maia' },
        state: 'open',
      },
    ],
  },
  {
    source: 'github',
    table: 'issues',
    method: 'GET',
    path: '/repos/oxide/coral/issues?labels=bug&per_page=15',
    durationMs: 92,
    responseBody: [
      { number: 88, title: 'Trace detail panel clips response JSON', labels: ['bug', 'ui'] },
    ],
  },
  {
    source: 'slack',
    table: 'conversations',
    method: 'GET',
    path: '/api/conversations.list?types=public_channel,private_channel',
    durationMs: 68,
    responseBody: {
      ok: true,
      channels: [
        { id: 'C08CORAL', name: 'eng-coral' },
        { id: 'C08RELEASE', name: 'release-coordination' },
      ],
    },
  },
  {
    source: 'slack',
    table: 'messages',
    method: 'GET',
    path: '/api/conversations.history?channel=C08CORAL&limit=50',
    durationMs: 126,
    responseBody: {
      ok: true,
      messages: [
        {
          user: 'U01ALICE',
          text: 'Playwright trace review is ready for feedback',
          ts: '1763999999.000100',
        },
        {
          user: 'U02BOB',
          text: 'Can we include Linear and GitHub spans?',
          ts: '1763999988.000200',
        },
      ],
    },
  },
  {
    source: 'linear',
    table: 'users',
    method: 'POST',
    path: '/graphql',
    durationMs: 49,
    requestBody: {
      operationName: 'UsersForAssignees',
      query: `query UsersForAssignees($first: Int!) {
  users(first: $first) {
    nodes {
      name
    }
  }
}`,
      variables: { first: 25 },
    },
    responseBody: { data: { users: { nodes: [{ name: 'Avery Chen' }, { name: 'Mina Park' }] } } },
  },
  {
    source: 'github',
    table: 'actions_runs',
    method: 'GET',
    path: '/repos/oxide/coral/actions/runs?per_page=20',
    durationMs: 141,
    responseBody: {
      total_count: 2,
      workflow_runs: [
        { name: 'Validate', conclusion: 'success' },
        { name: 'Release', conclusion: 'skipped' },
      ],
    },
  },
  {
    source: 'slack',
    table: 'reactions',
    method: 'GET',
    path: '/api/reactions.get?channel=C08CORAL&timestamp=1763999999.000100',
    durationMs: 44,
    responseBody: {
      ok: true,
      message: {
        reactions: [
          { name: 'eyes', count: 3 },
          { name: 'shipit', count: 1 },
        ],
      },
    },
  },
  {
    source: 'linear',
    table: 'projects',
    method: 'POST',
    path: '/graphql',
    durationMs: 61,
    requestBody: {
      operationName: 'OpenProjects',
      query: `query OpenProjects($includeArchived: Boolean!) {
  projects(includeArchived: $includeArchived) {
    nodes {
      name
    }
  }
}`,
      variables: { includeArchived: false },
    },
    responseBody: {
      data: { projects: { nodes: [{ name: 'Coral UI' }, { name: 'Source Runtime' }] } },
    },
  },
  {
    source: 'github',
    table: 'issue_previews',
    method: 'GET',
    path: '/repos/oxide/coral/issues?labels=bug&per_page=15&preview=malformed',
    durationMs: 74,
    responseBody: '{"oops":',
  },
  {
    source: 'github',
    table: 'repository_search',
    method: 'POST',
    path: '/api/v1/search',
    durationMs: 66,
    requestBody: {
      operationName: 'RepositorySearch',
      query: `query RepositorySearch($name: String!) {
  repository(name: $name) {
    id
    name
    isPrivate
  }
}`,
      variables: { name: 'coral' },
    },
    responseBody: {
      data: {
        repository: {
          id: 'R_kgDOExample',
          name: 'coral',
          isPrivate: false,
        },
      },
      errors: [{ message: 'GraphQL warnings should still be visible', path: ['repository'] }],
    },
  },
  {
    source: 'github',
    table: 'pull_request_archive',
    method: 'GET',
    path: '/repos/oxide/coral/pulls?state=closed&per_page=20',
    durationMs: 88,
    responseBodyTruncated: true,
    responseBodySize: 4096,
  },
  {
    source: 'linear',
    table: 'issue_request_preview',
    method: 'POST',
    path: '/graphql',
    durationMs: 52,
    requestBodyPresent: true,
    requestBodySize: 2048,
    responseBody: {
      data: {
        issues: {
          nodes: [
            {
              identifier: 'CORAL-201',
              title: 'Request body present fallback',
              priorityLabel: 'Low',
            },
          ],
        },
      },
    },
  },
]

function nanos(offsetMs: number): string {
  return (BASE_TIME_NANOS + BigInt(offsetMs) * 1_000_000n).toString()
}

function sourceHost(source: SourceName): string {
  switch (source) {
    case 'github':
      return 'api.github.com'
    case 'linear':
      return 'api.linear.app'
    case 'slack':
      return 'slack.com'
  }
}

function traceSummary(index: number, fixture: TraceFixture): TraceSummary {
  const displayIndex = index + 1
  const traceId = traceIdForIndex(index)
  const durationNanos = `${fixture.durationMs * 1_000_000}`

  return create(TraceSummarySchema, {
    traceId,
    rootSpanId: `${traceId}-root`,
    name: 'coral.query',
    query: fixture.query,
    status: fixture.status ?? TraceStatus.OK,
    startTimeUnixNanos: nanos(displayIndex * 1_000),
    endTimeUnixNanos: nanos(displayIndex * 1_000 + fixture.durationMs),
    durationNanos,
    spanCount: index === selectedTraceIndex ? selectedTraceSpans.length : 3,
    rowCount: `${fixture.rows ?? 0}`,
    rowCountRecorded: fixture.rows !== undefined,
  })
}

function traceIdForIndex(index: number): string {
  return `trace-${(index + 1).toString().padStart(2, '0')}`
}

function httpSpan(traceId: string, fixture: SpanFixture, index: number): TraceSpan {
  const startOffsetMs = 7_000 + index * 37
  const statusCode = fixture.statusCode ?? 200
  const attrs: Record<string, unknown> = {
    'http.request.method': fixture.method,
    'http.response.status_code': statusCode,
    'url.full': `https://${sourceHost(fixture.source)}${fixture.path}`,
    'coral.source': fixture.source,
    'coral.table': fixture.table,
  }

  if (fixture.requestBody) {
    attrs['http.request.body.present'] = true
  } else if (fixture.requestBodyPresent !== undefined) {
    attrs['http.request.body.present'] = fixture.requestBodyPresent
  }
  if (fixture.requestBodySize !== undefined) {
    attrs['http.request.body.size'] = `${fixture.requestBodySize}`
  }
  if (fixture.requestBodyTruncated) {
    attrs['coral.http.request.body.truncated'] = true
  }
  if (fixture.responseBodyPresent !== undefined) {
    attrs['http.response.body.present'] = fixture.responseBodyPresent
  }
  if (fixture.responseBodySize !== undefined) {
    attrs['http.response.body.size'] = `${fixture.responseBodySize}`
  }
  if (fixture.responseBodyTruncated) {
    attrs['coral.http.response.body.truncated'] = true
  }

  return create(TraceSpanSchema, {
    traceId,
    spanId: `${traceId}-span-${index}`,
    parentSpanId: '',
    name: `http.${fixture.method.toLowerCase()}`,
    kind: 'client',
    status: statusCode >= 400 ? TraceStatus.ERROR : TraceStatus.OK,
    startTimeUnixNanos: nanos(startOffsetMs),
    endTimeUnixNanos: nanos(startOffsetMs + fixture.durationMs),
    durationNanos: `${fixture.durationMs * 1_000_000}`,
    attributesJson: JSON.stringify(attrs),
    eventsJson: '[]',
    linksJson: '[]',
    resourceJson: JSON.stringify({ 'service.name': 'coral' }),
    scopeName: `coral-source-${fixture.source}`,
  })
}

function bodySpan(
  traceId: string,
  parentSpanId: string,
  fixture: SpanFixture,
  index: number,
  kind: 'request' | 'response',
): TraceSpan | undefined {
  const body = kind === 'request' ? fixture.requestBody : fixture.responseBody
  if (body === undefined) return undefined

  const bodyAttr = kind === 'request' ? 'coral.http.request.body' : 'coral.http.response.body'
  const truncatedAttr =
    kind === 'request' ? 'coral.http.request.body.truncated' : 'coral.http.response.body.truncated'
  const truncated =
    kind === 'request' ? fixture.requestBodyTruncated : fixture.responseBodyTruncated
  const startOffsetMs = 7_000 + index * 37 + (kind === 'request' ? 1 : 2)
  const attrs: Record<string, unknown> = {
    target: 'coral.http.body',
    'coral.http.body.direction': kind,
    [bodyAttr]: JSON.stringify(body),
    [truncatedAttr]: Boolean(truncated),
  }

  return create(TraceSpanSchema, {
    traceId,
    spanId: `${parentSpanId}-${kind}-body`,
    parentSpanId,
    name: `coral.http.${kind}.body`,
    kind: 'internal',
    status: TraceStatus.OK,
    startTimeUnixNanos: nanos(startOffsetMs),
    endTimeUnixNanos: nanos(startOffsetMs),
    durationNanos: '0',
    attributesJson: JSON.stringify(attrs),
    eventsJson: '[]',
    linksJson: '[]',
    resourceJson: JSON.stringify({ 'service.name': 'coral' }),
    scopeName: 'coral',
  })
}

function spansForFixture(traceId: string, fixture: SpanFixture, index: number): TraceSpan[] {
  const span = httpSpan(traceId, fixture, index)
  return [
    span,
    bodySpan(traceId, span.spanId, fixture, index, 'request'),
    bodySpan(traceId, span.spanId, fixture, index, 'response'),
  ].filter((candidate): candidate is TraceSpan => candidate !== undefined)
}

const selectedTraceSpans = detailSpans.flatMap((fixture, index) =>
  spansForFixture(traceIdForIndex(selectedTraceIndex), fixture, index + 1),
)

export const tenTraceList: TraceSummary[] = traceFixtures.map((fixture, index) =>
  traceSummary(index, fixture),
)
export const selectedTrace = tenTraceList[selectedTraceIndex]

export const traceListResponse: ListTracesResponse = create(ListTracesResponseSchema, {
  traces: tenTraceList,
})

export const emptyTraceListResponse: ListTracesResponse = create(ListTracesResponseSchema, {
  traces: [],
})

export const selectedTraceDetailResponse: GetTraceResponse = create(GetTraceResponseSchema, {
  summary: selectedTrace,
  spans: selectedTraceSpans,
})
