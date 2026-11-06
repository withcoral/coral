import type { ShouldRevalidateFunctionArgs } from 'react-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { OrganizationRole } from '@/generated/coral/management/v1/organization_membership_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import type { ReadableSpan, SpanExporter } from '@opentelemetry/sdk-trace'
import { createRequestTracing } from '../../server-tracing.js'
import { loader, shouldRevalidate } from './app-shell'

const transport = fakeManagementTransport({
  workspaces: {
    listWorkspaces: ({ pageToken }) =>
      pageToken === ''
        ? {
            workspaces: [workspace(1), workspace(2, WorkspaceState.ARCHIVED)],
            nextPageToken: 'p2',
          }
        : { workspaces: [workspace(3)], nextPageToken: '' },
  },
  organizationMemberships: { getCurrentOrganizationMembership: () => ({ role }) },
  activity: {
    countRequiredActions: async () => {
      countStarted()
      await countGate
      return {
        actionCount: 1n,
        approvalCount: 1n,
        connectionCount: 0n,
        approvalRevision: 'a',
        connectionRevision: 'c',
      }
    },
  },
  releases: {
    getNewerRelease: () => {
      if (release === 'unavailable') throw new ConnectError('down', Code.Unavailable)
      return { release }
    },
  },
})
let role = OrganizationRole.UNSPECIFIED
let countGate: Promise<void> = Promise.resolve()
let countStarted = () => {}
let release: { version: string; notesUrl: string } | undefined | 'unavailable'
vi.mock('@/lib/coral-request.server', () => ({ managementTransportForRequest: () => transport }))

function workspace(id: number, state = WorkspaceState.ACTIVE) {
  return {
    name: `workspaces/00000000-0000-4000-8000-00000000000${id}`,
    displayName: `Workspace ${id}`,
    state,
    effectiveRole: WorkspaceRole.MEMBER,
    mcpEndpoint: `https://coral.example.com/mcp/${id}`,
    etag: `etag-${id}`,
  }
}

// What the sidebar shows for the fixture Workspace `id`.
function sidebarEntry(id: number, state: 'active' | 'archived') {
  return {
    displayName: `Workspace ${id}`,
    path: `/workspaces/00000000-0000-4000-8000-00000000000${id}`,
    effectiveRole: 'member',
    mcpEndpoint: `https://coral.example.com/mcp/${id}`,
    etag: `etag-${id}`,
    state,
  }
}

it('lists every Workspace across pages, archived ones apart, because the sidebar is the only listing', async () => {
  const result = await loader(authRouteTestArgs(new Request('http://reef/activity'), {}, null))
  expect(result.data.workspaces).toEqual([sidebarEntry(1, 'active'), sidebarEntry(3, 'active')])
  expect(result.data.archivedWorkspaces).toEqual([sidebarEntry(2, 'archived')])
})

it.each([
  ['an owner, with a newer release', OrganizationRole.OWNER, '1.4.0', true],
  ['a member, with a newer release', OrganizationRole.MEMBER, '1.4.0', false],
  ['an owner, with nothing newer', OrganizationRole.OWNER, undefined, false],
  ['an owner, when Coral cannot answer', OrganizationRole.OWNER, 'unavailable', false],
] as const)(
  'offers the release notes to %s only when an owner can act on them',
  async (_case, caseRole, version, shown) => {
    role = caseRole
    const notesUrl = 'https://github.com/withcoral/coral/releases/tag/v1.4.0'
    release = version === '1.4.0' ? { version, notesUrl } : version
    const result = await loader(authRouteTestArgs(new Request('http://reef/activity'), {}, null))
    expect(result.data.newerRelease).toEqual(shown ? { version: '1.4.0', notesUrl } : undefined)
  },
)

// Catches: a shell count read outside its span, which would leave its duration unmeasured.
it('reads the action counts inside one span that ends with the read', async () => {
  const spans: ReadableSpan[] = []
  const spanExporter: SpanExporter = {
    export(batch, done) {
      spans.push(...batch)
      done({ code: 0 })
    },
    shutdown: async () => undefined,
  }
  const counted = () => spans.filter((span) => span.name === 'app-shell action counts').length
  const tracing = createRequestTracing({ env: {}, spanExporter })
  const request = new Request('http://reef/activity')
  let open!: () => void
  countGate = new Promise((resolve) => (open = resolve))
  let reading!: () => void
  const read = new Promise<void>((resolve) => (reading = resolve))
  countStarted = reading
  let result: Awaited<ReturnType<typeof loader>> | undefined
  try {
    await tracing.traceRequest(request, async () => {
      const loading = loader(authRouteTestArgs(request, {}, null))
      await read
      await tracing.flush()
      expect(counted()).toBe(0)
      open()
      result = await loading
    })
    await tracing.flush()
  } finally {
    countGate = Promise.resolve()
    countStarted = () => {}
    await tracing.shutdown()
  }
  expect(counted()).toBe(1)
  expect(result?.data.actionCounts).toBeTruthy()
})

// A row opens its review through the query. The sidebar must not reload for it, but a refresh
// and a move to another page must still read the shell.
it.each([
  ['opening a review', '/workspaces/one/approvals', '/workspaces/one/approvals?request=a', false],
  ['closing a review', '/workspaces/one/approvals?request=a', '/workspaces/one/approvals', false],
  ['refreshing the open review', '/workspaces/one/approvals?request=a', '', true],
  ['leaving for another page', '/workspaces/one/approvals', '/workspaces/one/activity', true],
  [
    'changing the query of another page',
    '/workspaces/one/activity',
    '/workspaces/one/activity?x=1',
    true,
  ],
])('%s', (_, from, to, expected) => {
  const currentUrl = new URL(from, 'http://reef.test')
  const nextUrl = new URL(to || from, 'http://reef.test')
  expect(
    shouldRevalidate({
      currentUrl,
      nextUrl,
      defaultShouldRevalidate: true,
    } as ShouldRevalidateFunctionArgs),
  ).toBe(expected)
})
