import { create } from '@bufbuild/protobuf'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { createContext, useContext } from 'react'
import { createRoutesStub } from 'react-router'

import {
  TraceInvocationKind,
  TraceOperationKind,
  TraceStatus,
  TraceSummarySchema,
} from '@/generated/coral/v1/traces_pb'

import { TracesIndex } from './traces-index'
import type { TraceSummaryData } from './trace-utils'

const referenceTimeMs = Date.parse('2026-08-21T12:00:00Z')

function trace(
  traceId: string,
  startedSecondsAgo: number,
  values: Partial<TraceSummaryData>,
): TraceSummaryData {
  const startTimeUnixNanos = BigInt(referenceTimeMs - startedSecondsAgo * 1_000) * 1_000_000n
  return create(TraceSummarySchema, {
    durationNanos: '42000000',
    endTimeUnixNanos: (startTimeUnixNanos + 42_000_000n).toString(),
    invocationKind: TraceInvocationKind.DIRECT,
    name: 'coral.query',
    operationKind: TraceOperationKind.QUERY,
    operationName: 'sql',
    rootSpanId: `${traceId}-root`,
    spanCount: 4,
    startTimeUnixNanos: startTimeUnixNanos.toString(),
    status: TraceStatus.OK,
    traceId,
    ...values,
  })
}

const traces = [
  trace('01-query', 8, {
    durationNanos: '184000000',
    query: 'SELECT name, stargazer_count FROM github.repositories ORDER BY stargazer_count DESC',
    rowCount: '25',
    rowCountRecorded: true,
  }),
  trace('02-search', 74, {
    durationNanos: '38000000',
    invocationKind: TraceInvocationKind.MCP,
    operationKind: TraceOperationKind.SEARCH,
    operationName: 'search',
    query: 'recent deployment failures',
  }),
  trace('03-tool', 310, {
    durationNanos: '1420000000',
    invocationKind: TraceInvocationKind.MCP,
    operationKind: TraceOperationKind.TOOL,
    operationName: 'list_columns',
    status: TraceStatus.ERROR,
  }),
]

const tracesWithUsers = [
  { ...traces[0], user: { id: 'ludo' } },
  { ...traces[1], user: { id: 'coral:local' } },
  traces[2],
]

const TraceStoryContext = createContext<React.ComponentProps<typeof TracesIndex> | null>(null)

function TraceStoryRoute() {
  const args = useContext(TraceStoryContext)
  if (!args) throw new Error('trace story args are unavailable')
  return <TracesIndex {...args} />
}

const RoutesStub = createRoutesStub([
  {
    Component: TraceStoryRoute,
    path: '/workspaces/:workspaceId/traces',
  },
])

function TraceStory(args: React.ComponentProps<typeof TracesIndex>) {
  return (
    <TraceStoryContext value={args}>
      <RoutesStub initialEntries={['/workspaces/default/traces']} />
    </TraceStoryContext>
  )
}

const meta = {
  args: {
    endpointLabel: '127.0.0.1:50051',
    loadError: null,
    referenceTimeMs,
    traces,
    workspaceId: 'default',
  },
  component: TracesIndex,
  parameters: { layout: 'fullscreen' },
  render: (args) => (
    <div style={{ height: '100dvh' }}>
      <TraceStory {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Views/Traces',
} satisfies Meta<typeof TracesIndex>

export default meta
type Story = StoryObj<typeof meta>

export const Populated: Story = {}

export const WithUsers: Story = { args: { traces: tracesWithUsers } }

export const Empty: Story = { args: { traces: [] } }

export const Disconnected: Story = {
  args: {
    loadError: 'Could not connect to Coral. Retrying automatically.',
  },
}
