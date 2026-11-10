import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { AuthoredToolDiagnosticCategory } from '@/generated/coral/management/v1/authored_tool_pb'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { coralRefusal, fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader, shouldRevalidate } from './shared-tool-publish'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

const workspace = '00000000-0000-4000-8000-000000000001'
const parent = `workspaces/${workspace}`
const page = `/workspaces/${workspace}`
const workerId = '00000000-0000-4000-8000-0000000000b1'
const remind = `${parent}/sharedTools/00000000-0000-4000-8000-0000000000d1`

// The bytes keep their exact spelling, trailing space and final newline included.
const SOURCE = 'export default async function remind(name: string) {\n  return name  \n}\n'
const DECLARATION = 'export default function remind(name: string): Promise<string>;\n'
const PUBLISHED = {
  revision: 2n,
  description: 'Remind someone, as published',
  source: 'export default async () => "published"\n',
  declaration: 'export default function (): Promise<string>;\n',
  defaultAccessState: ToolAccessState.DISABLED,
  publisher: 'users/ada',
  publishTime: { seconds: 1_762_700_000n, nanos: 0 },
}
const FIRST = {
  revision: 1n,
  description: 'Remind someone, at first',
  source: 'export default async () => "first"\n',
  declaration: 'export default function (): Promise<string>;\n',
  defaultAccessState: ToolAccessState.ENABLED,
}

type Publish = {
  path: string
  validateOnly: boolean
  expectedRevision: bigint
  [field: string]: unknown
}
let publishes: Publish[]
/** What Coral answers to a publish; a test replaces it to refuse one. */
let answer: (request: Publish) => object

beforeEach(() => {
  publishes = []
  answer = (request) =>
    request.validateOnly
      ? {
          review: {
            previousRevision: request.path === 'mail.remind' ? PUBLISHED : undefined,
            declaration: DECLARATION,
            diagnostics: [
              {
                category: AuthoredToolDiagnosticCategory.WARNING,
                code: 6133n,
                message: 'Unused.',
                line: 2n,
              },
            ],
            calledTools: ['coral.providers.mailer.send', 'coral.shared.mail.send'],
            sharedTools: [{ path: 'mail.send', revision: 4n }],
            runtimeBuiltPaths: ['coral.providers.demo'],
          },
        }
      : {
          sharedTool: {
            path: request.path,
            currentRevision: { revision: request.expectedRevision + 1n },
          },
        }
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Research',
        effectiveRole: WorkspaceRole.OWNER,
        state: WorkspaceState.ACTIVE,
      }),
    },
    memberships: {
      listWorkspaceOwners: () => ({
        owners: [{ user: 'users/ada', profile: { user: 'users/ada', displayName: 'Ada' } }],
      }),
    },
    privateTools: {
      getPrivateTool: ({ path, agent }) => {
        if (path !== 'people.remind') throw new ConnectError('NOT_FOUND', Code.NotFound)
        return {
          path,
          description: agent ? 'Remind someone, by the worker' : 'Remind someone',
          source: SOURCE,
        }
      },
    },
    sharedTools: {
      getSharedTool: ({ path }) => {
        if (path === 'mail..remind')
          throw new ConnectError('INVALID_ARGUMENT', Code.InvalidArgument)
        if (path !== 'mail.remind') throw new ConnectError('NOT_FOUND', Code.NotFound)
        return { name: remind, path, currentRevision: PUBLISHED }
      },
      getSharedToolRevision: ({ sharedTool, revision }) => {
        if (sharedTool !== remind || revision !== 1n)
          throw new ConnectError('NOT_FOUND', Code.NotFound)
        return FIRST
      },
      publishSharedTool: (request) => {
        const publish = { ...request } as unknown as Publish
        publishes.push(publish)
        return answer(publish)
      },
    },
  })
})

const review = async (query: string) =>
  (await loader(
    authRouteTestArgs(
      new Request(`http://reef${page}/shared-tools/publish?${query}`),
      { workspace },
      null,
    ) as never,
  ))!.data

const post = (fields: Record<string, string>) => {
  const form = new FormData()
  for (const [name, value] of Object.entries(fields)) form.set(name, value)
  return action(
    authRouteTestArgs(
      new Request(`http://reef${page}/shared-tools/publish`, {
        method: 'POST',
        headers: { origin: 'http://reef' },
        body: form,
      }),
      { workspace },
      null,
    ) as never,
  )
}

// Catches: a review of bytes other than the private tool's, a review that
// writes, a first publish that expects a revision, or a review that drops
// what the owner approves.
it('reviews a private tool at its own path as the first revision', async () => {
  const result = await review('tool=people.remind')
  expect(publishes).toMatchObject([
    {
      parent,
      path: 'people.remind',
      origin: { case: 'privateTool', value: { path: 'people.remind', agent: '' } },
      description: 'Remind someone',
      source: SOURCE,
      expectedRevision: 0n,
      validateOnly: true,
    },
  ])
  expect(result).toEqual({
    refusal: null,
    review: {
      origin: { kind: 'private', path: 'people.remind', agent: null },
      path: 'people.remind',
      description: 'Remind someone',
      source: SOURCE,
      declaration: DECLARATION,
      diagnostics: [{ category: 'warning', code: 6133, message: 'Unused.', line: 2, column: null }],
      diagnosticsTruncated: false,
      previous: null,
      calledTools: ['coral.providers.mailer.send', 'coral.shared.mail.send'],
      sharedTools: [{ path: 'mail.send', revision: 4 }],
      runtimeBuiltPaths: ['coral.providers.demo'],
      defaultPolicy: null,
    },
  })
})

// Catches: a review against the wrong revision, which Coral refuses as moved;
// a diff with no other side; a replaced revision that does not say whose it
// is; an agent's tool reviewed as the owner's own; or a new revision that
// loses the policy the shared tool has.
it('reviews an agent’s private tool at the path of a shared tool', async () => {
  const result = await review(`tool=people.remind&agent=${workerId}&path=mail.remind`)
  expect(publishes).toMatchObject([
    {
      path: 'mail.remind',
      origin: {
        case: 'privateTool',
        value: { path: 'people.remind', agent: `${parent}/agents/${workerId}` },
      },
      description: 'Remind someone, by the worker',
      expectedRevision: 2n,
      validateOnly: true,
    },
  ])
  expect(result.review).toMatchObject({
    origin: { kind: 'private', path: 'people.remind', agent: workerId },
    path: 'mail.remind',
    previous: {
      revision: 2,
      description: PUBLISHED.description,
      source: PUBLISHED.source,
      declaration: PUBLISHED.declaration,
      publisher: { user: 'users/ada', name: 'Ada', email: '', local: false },
      publishedAt: '2025-11-09T14:53:20.000Z',
    },
    defaultPolicy: 'disabled',
  })
})

// Catches: a restore that reviews the current bytes instead of the
// revision's, that does not name the revision it restores, or that starts on
// the older revision's policy, so a rollback loosens the policy unnoticed.
it('reviews a restore with the bytes of its revision and the policy the tool has now', async () => {
  const result = await review('restore=1&path=mail.remind')
  expect(publishes).toMatchObject([
    {
      path: 'mail.remind',
      origin: { case: 'restoreRevision', value: 1n },
      description: FIRST.description,
      source: FIRST.source,
      expectedRevision: 2n,
      validateOnly: true,
    },
  ])
  expect(result.review).toMatchObject({
    origin: { kind: 'restore', revision: 1 },
    description: FIRST.description,
    source: FIRST.source,
    previous: { revision: 2 },
    defaultPolicy: 'disabled',
  })
})

// Catches: a refused review that fails the page instead of telling the owner
// what to change in the dialog, or that drops Coral's own text of the fault.
it.each([
  {
    name: 'a path that is not a shared tool path',
    query: 'tool=people.remind&path=mail..remind',
    refuse: () =>
      coralRefusal(Code.InvalidArgument, 'SHARED_TOOL_PATH_INVALID', {
        message: 'Shared Tool Path must contain 1 through 16 segments',
      }),
    refusal:
      'Enter a path of 1 to 16 names with a dot between each, such as mail.remind. Each name is a JavaScript identifier.',
  },
  {
    name: 'a path inside another shared tool',
    query: 'tool=people.remind&path=mail',
    refuse: () => coralRefusal(Code.FailedPrecondition, 'SHARED_TOOL_PATH_TAKEN'),
    refusal: 'Another shared tool uses this path, or a part of it. Enter another path.',
  },
  {
    name: 'a member',
    query: 'tool=people.remind',
    refuse: () => coralRefusal(Code.PermissionDenied, 'PERMISSION_DENIED'),
    refusal: 'Only an owner can publish to shared tools.',
  },
  {
    name: 'a private tool that is gone',
    query: 'tool=people.gone',
    refuse: null,
    refusal: 'The private tool or the revision no longer exists. Close this dialog.',
  },
  {
    name: 'a restore of a shared tool that is gone',
    query: 'restore=1&path=mail.gone',
    refuse: null,
    refusal: 'This shared tool no longer exists. Close this dialog.',
  },
])('returns the refusal of a review of $name as text', async ({ query, refuse, refusal }) => {
  if (refuse)
    answer = () => {
      throw refuse()
    }
  expect(await review(query)).toEqual({
    review: null,
    path: new URLSearchParams(query).get('path') ?? new URLSearchParams(query).get('tool'),
    refusal,
  })
})

// Catches: an address that names nothing to copy reaching Coral.
it.each(['', 'restore=0&path=mail.remind', 'restore=one&path=mail.remind', 'tool=a&agent=%20'])(
  'refuses a review address that names no origin: "%s"',
  async (query) => {
    await expect(review(query)).rejects.toMatchObject({
      status: query.includes('agent') ? 404 : 400,
    })
    expect(publishes).toEqual([])
  },
)

// Catches: every change on the page, and each publish that succeeds, running
// one more review, which compiles the source for nobody.
it('reviews only when the dialog asks', () => {
  expect(shouldRevalidate()).toBe(false)
})

/** The reviewed content as the dialog sends it. */
const content = (reviewed: object) => JSON.stringify(reviewed)
const reviewed = {
  tool: 'people.remind',
  path: 'mail.remind',
  reviewed: content({ description: 'Remind someone', source: SOURCE, declaration: DECLARATION }),
  expectedRevision: '2',
  access: 'approval_required',
}

// Catches: a publish of anything but the reviewed bytes, such as a source
// whose line breaks the form post rewrote; a publish that drops the
// declaration or the revision the owner saw; or one that leaves the owner in
// the dialog after it succeeds.
it('publishes the exact reviewed bytes and opens the shared tool', async () => {
  const response = await post({ ...reviewed, agent: workerId })
  expect(publishes).toMatchObject([
    {
      parent,
      path: 'mail.remind',
      origin: {
        case: 'privateTool',
        value: { path: 'people.remind', agent: `${parent}/agents/${workerId}` },
      },
      description: 'Remind someone',
      source: SOURCE,
      reviewedDeclaration: DECLARATION,
      defaultAccessState: ToolAccessState.APPROVAL_REQUIRED,
      expectedRevision: 2n,
      validateOnly: false,
    },
  ])
  expect(response).toBeInstanceOf(Response)
  expect((response as Response).headers.get('location')).toBe(
    `${page}/tools/shared/mail.remind?sharedToolPublished=3`,
  )
})

// Catches: a restore published as a copy of a private tool.
it('publishes a restore of the revision the form names', async () => {
  const { tool: _tool, ...restore } = reviewed
  await post({
    ...restore,
    restore: '1',
    reviewed: content({
      description: FIRST.description,
      source: FIRST.source,
      declaration: FIRST.declaration,
    }),
  })
  expect(publishes).toMatchObject([
    { origin: { case: 'restoreRevision', value: 1n }, source: FIRST.source, validateOnly: false },
  ])
})

// Catches: a form that is not a review's reaching Coral as a publish.
it.each([
  { name: 'a policy Coral does not know', fields: { access: 'maybe' } },
  { name: 'a revision that is not a number', fields: { expectedRevision: 'two' } },
  { name: 'no origin', fields: { tool: '' } },
  {
    name: 'a source over Coral’s limit',
    fields: {
      reviewed: content({ description: '', source: 'x'.repeat(64 * 1024 + 1), declaration: '' }),
    },
  },
  { name: 'reviewed content that is not JSON', fields: { reviewed: 'export default 1' } },
  { name: 'reviewed content with no source', fields: { reviewed: content({ description: '' }) } },
])('refuses a publish form with $name and sends Coral nothing', async ({ fields }) => {
  await expect(post({ ...reviewed, ...fields })).rejects.toMatchObject({ status: 400 })
  expect(publishes).toEqual([])
})

// Catches: a refused publish that leaves the dialog, that shows a reason
// code, or that does not tell the owner to review again when another owner
// published first or a called tool changed the declaration.
it.each([
  {
    name: 'another owner published first',
    refuse: () => coralRefusal(Code.Aborted, 'ABORTED'),
    status: 409,
    error: 'Another owner published this shared tool first. Review the changes again.',
  },
  {
    name: 'the declaration changed since the review',
    refuse: () => coralRefusal(Code.FailedPrecondition, 'SHARED_TOOL_DECLARATION_CHANGED'),
    status: 409,
    error: 'A tool that this source calls changed after your review. Review the declaration again.',
  },
  {
    name: 'the path is taken',
    refuse: () => coralRefusal(Code.FailedPrecondition, 'SHARED_TOOL_PATH_TAKEN'),
    status: 409,
    error: 'Another shared tool uses this path, or a part of it. Enter another path.',
  },
  {
    name: 'the private tool is gone',
    refuse: () => coralRefusal(Code.FailedPrecondition, 'SHARED_TOOL_SOURCE_UNREADABLE'),
    status: 409,
    error: 'The private tool or the revision no longer exists. Close this dialog.',
  },
  {
    name: 'the source cannot become a tool',
    refuse: () =>
      coralRefusal(Code.InvalidArgument, 'SHARED_TOOL_SOURCE_REJECTED', {
        message: 'Shared Tool coral.shared.mail.remind authoring failed: no default export',
      }),
    status: 400,
    error:
      'Correct the source in the private tool. Shared Tool coral.shared.mail.remind authoring failed: no default export',
  },
  {
    name: 'the restore is not of its revision',
    refuse: () =>
      coralRefusal(Code.InvalidArgument, 'SHARED_TOOL_RESTORE_DIFFERS', {
        message: 'a restore publishes the exact source and description of the revision it names',
      }),
    status: 400,
    error: 'This content is not that of the revision you restore. Review the revision again.',
  },
  {
    name: 'the caller is a member',
    refuse: () => coralRefusal(Code.PermissionDenied, 'PERMISSION_DENIED'),
    status: 403,
    error: 'Only an owner can publish to shared tools.',
  },
  {
    name: 'the workspace is archived',
    refuse: () => coralRefusal(Code.FailedPrecondition, 'WORKSPACE_ARCHIVED'),
    status: 409,
    error: 'This workspace is archived. Restore it before you make changes.',
  },
  {
    name: 'Coral is not available',
    refuse: () => new ConnectError('UNAVAILABLE', Code.Unavailable),
    status: 503,
    error: 'Coral could not publish this shared tool. Coral is not available.',
  },
])('keeps a publish refused because $name in the dialog', async ({ refuse, status, error }) => {
  answer = () => {
    throw refuse()
  }
  expect(await post(reviewed)).toMatchObject({ init: { status }, data: { error } })
})
