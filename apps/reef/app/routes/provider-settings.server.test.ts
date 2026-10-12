import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'

import {
  CredentialRoute_Kind,
  ProviderBlocker,
  ProviderSetupInputKind,
  ProviderState,
} from '@/generated/coral/management/v1/provider_pb'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '@/generated/coral/management/v1/account_pb'
import { loader } from './provider-settings'

const mocks = vi.hoisted(() => ({ workspace: vi.fn() }))
vi.mock('@/lib/workspace-route.server', () => ({ authorizedWorkspace: mocks.workspace }))
vi.mock('@/lib/management.server', async (original) => ({
  ...(await original<typeof import('@/lib/management.server')>()),
  formContext: () => ({ csrf: 'csrf', requestId: 'request' }),
}))

const workspaceId = '29a180e5-397c-4844-92de-8bbea6066235'
const providerId = '11111111-1111-4111-8111-111111111111'
const name = `workspaces/${workspaceId}/providers/${providerId}`

let calls: string[]
let responses: Record<string, (request: unknown) => unknown>

/** Every Coral call the loader makes, by service and method, whatever it is. */
function recordingApi() {
  return new Proxy(
    {},
    {
      get: (_, service: string) =>
        new Proxy(
          {},
          {
            get: (__, method: string) => async (request: unknown) => {
              calls.push(`${service}.${method}`)
              const respond = responses[`${service}.${method}`]
              if (!respond) throw new Error(`Unexpected call ${service}.${method}`)
              return respond(request)
            },
          },
        ),
    },
  )
}

function signedInAs(role: 'owner' | 'member', state: 'active' | 'archived' = 'active') {
  mocks.workspace.mockResolvedValue({
    api: recordingApi(),
    workspace: {
      name: `workspaces/${workspaceId}`,
      path: `/workspaces/${workspaceId}`,
      effectiveRole: role,
      state,
    },
  })
}

const provider = (overrides: Record<string, unknown> = {}) => ({
  name,
  displayName: 'GitLab.com',
  namespace: 'gitlab',
  state: ProviderState.ACTIVE,
  etag: 'etag-1',
  credentialRoute: { kind: CredentialRoute_Kind.CALLER },
  blockers: [],
  ...overrides,
})

async function load() {
  const result = (await loader({
    request: new Request(
      `https://reef.test/workspaces/${workspaceId}/providers/${providerId}/settings`,
    ),
    context: {},
    params: { workspace: workspaceId, provider: providerId },
  } as never)) as unknown as { data: Record<string, unknown> }
  return result.data
}

beforeEach(() => {
  vi.resetAllMocks()
  calls = []
  responses = {
    'providers.getProvider': () => provider(),
    'providers.getProviderSource': () => ({
      url: 'https://reader:secret@gitlab.example.com/api/v4/openapi.json?private_token=abc',
      kind: ProviderSetupInputKind.OPENAPI,
    }),
    'accounts.getProviderAccount': () => ({
      account: { state: ProviderAccountState.READY },
      canConnect: true,
    }),
    'providers.listOperations': () => ({ operations: [], nextPageToken: '' }),
  }
})

// Catches: opening settings creating a Setup, preparing an update, or calling
// any other write to fill in configuration it cannot read.
it('makes only read calls when settings open', async () => {
  signedInAs('owner')
  await load()
  expect(calls.toSorted()).toEqual([
    'accounts.getProviderAccount',
    'providers.getProvider',
    'providers.getProviderSource',
    'providers.listOperations',
  ])
})

// Catches: members reaching the owner-only source read, or its metadata.
it('does not read the source for a member', async () => {
  signedInAs('member')
  const data = await load()
  expect(calls).not.toContain('providers.getProviderSource')
  expect(data.source).toBeUndefined()
})

// Catches: the raw source URL, with its credentials, reaching the browser.
it('gives owners the source kind and a redacted URL', async () => {
  signedInAs('owner', 'archived')
  const data = await load()
  expect(data.source).toEqual({
    state: 'loaded',
    kind: 'openapi',
    location: {
      type: 'url',
      url: 'https://gitlab.example.com/api/v4/openapi.json?private_token=[redacted]',
      host: 'gitlab.example.com',
    },
  })
  expect(JSON.stringify(data)).not.toContain('secret')
  expect(JSON.stringify(data)).not.toContain('abc')
})

it('tells an uploaded document apart from a failed source read', async () => {
  signedInAs('owner')
  responses['providers.getProviderSource'] = () => ({
    url: '',
    kind: ProviderSetupInputKind.OPENAPI,
  })
  expect((await load()).source).toEqual({
    state: 'loaded',
    kind: 'openapi',
    location: { type: 'upload' },
  })

  responses['providers.getProviderSource'] = () => {
    throw new ConnectError('unavailable', Code.Unavailable)
  }
  const data = await load()
  expect(data.source).toEqual({ state: 'failed' })
  expect(data.provider).toMatchObject({ displayName: 'GitLab.com' })
})

// Catches: settings calling an account connected when it needs a reconnect,
// or offering Connect or Reconnect to someone Coral will refuse.
it('reads the account calls use from the account itself', async () => {
  const requests: unknown[] = []
  responses['accounts.getProviderAccount'] = (request) => {
    requests.push(request)
    return {
      account: { state: ProviderAccountState.RECONNECT_REQUIRED },
      canConnect: false,
      canReconnect: false,
    }
  }
  signedInAs('member')
  expect((await load()).provider).toMatchObject({
    account: 'caller',
    connection: 'needs_reconnect',
    canConnect: false,
    canReconnect: false,
  })
  expect(requests).toEqual([
    expect.objectContaining({ provider: name, scope: ProviderAccountScope.USER }),
  ])

  responses['accounts.getProviderAccount'] = () => ({ canConnect: true })
  expect((await load()).provider).toMatchObject({ connection: 'not_connected', canConnect: true })

  responses['accounts.getProviderAccount'] = () => ({
    account: { state: ProviderAccountState.DISCONNECTED },
    canConnect: true,
  })
  expect((await load()).provider).toMatchObject({ connection: 'not_connected' })
})

// Catches: an owner's shared account read as the owner's own, or members
// reaching the shared account, which only owners may inspect.
it('reads the shared account for owners only', async () => {
  const scopes: unknown[] = []
  responses['providers.getProvider'] = () =>
    provider({
      credentialRoute: { kind: CredentialRoute_Kind.FIXED },
      blockers: [ProviderBlocker.SHARED_ACCOUNT_REQUIRED],
    })
  responses['accounts.getProviderAccount'] = (request) => {
    scopes.push((request as { scope: ProviderAccountScope }).scope)
    return { account: { state: ProviderAccountState.READY }, canConnect: true }
  }
  signedInAs('owner')
  expect((await load()).provider).toMatchObject({ account: 'fixed', connection: 'connected' })
  expect(scopes).toEqual([ProviderAccountScope.SHARED])

  calls = []
  signedInAs('member')
  expect((await load()).provider).toMatchObject({
    account: 'fixed',
    connection: 'not_connected',
    canConnect: false,
  })
  expect(calls.toSorted()).toEqual(['providers.getProvider', 'providers.listOperations'])
})

it('reads no account when calls use none', async () => {
  responses['providers.getProvider'] = () =>
    provider({ credentialRoute: { kind: CredentialRoute_Kind.NONE } })
  signedInAs('member')
  await load()
  expect(calls.toSorted()).toEqual(['providers.getProvider', 'providers.listOperations'])
})

// Catches: a failed read of your own account reported as connected. Coral's
// blockers no longer report a User's own account, so they cannot stand in.
it('reports your own account as unknown when its read fails', async () => {
  responses['accounts.getProviderAccount'] = () => {
    throw new ConnectError('unavailable', Code.Unavailable)
  }
  signedInAs('member')
  expect((await load()).provider).toMatchObject({ connection: 'unknown' })
})

// Catches: a failed shared-account read losing what the provider's blockers
// still say about the shared account.
it('falls back to the shared-account blocker when the shared read fails', async () => {
  responses['accounts.getProviderAccount'] = () => {
    throw new ConnectError('unavailable', Code.Unavailable)
  }
  responses['providers.getProvider'] = () =>
    provider({
      credentialRoute: { kind: CredentialRoute_Kind.FIXED },
      blockers: [ProviderBlocker.SHARED_ACCOUNT_REQUIRED],
    })
  signedInAs('owner')
  expect((await load()).provider).toMatchObject({ connection: 'not_connected' })
})

// Catches: a disabled provider, or one with no callable operation, looking
// the same as a ready one in settings.
it('says whether the provider is enabled and has operations ready', async () => {
  responses['providers.getProvider'] = () =>
    provider({
      state: ProviderState.DISABLED,
      blockers: [ProviderBlocker.PROVIDER_DISABLED, ProviderBlocker.NO_OPERATIONS_READY],
    })
  signedInAs('member')
  expect((await load()).provider).toMatchObject({
    enabled: false,
    operationsReady: false,
  })

  responses['providers.getProvider'] = () => provider()
  expect((await load()).provider).toMatchObject({ enabled: true, operationsReady: true })
})

// Catches: the summary counting one page of a long catalog, counting a
// blocked operation as callable, or counting a hint the provider did not
// declare true.
type Hints = { readOnly?: boolean; destructive?: boolean }
const readOnly: Hints = { readOnly: true }
const destructive: Hints = { readOnly: false, destructive: true }
const operation = (hints: Hints | undefined, available = true, tags: string[] = []) => ({
  hints,
  available,
  tags,
  relativePath: 'op',
})

it('counts every operation by declared hint and availability', async () => {
  const pages: Record<string, { operations: unknown[]; nextPageToken: string }> = {
    '': {
      operations: [
        operation(readOnly, true, ['issues']),
        operation(readOnly, false, ['issues']),
        operation(destructive, true, ['repos']),
      ],
      nextPageToken: 'next',
    },
    next: {
      operations: [operation({ destructive: false }), operation(undefined, false)],
      nextPageToken: '',
    },
  }
  responses['providers.listOperations'] = (request) =>
    pages[(request as { pageToken: string }).pageToken]
  signedInAs('member')
  expect((await load()).provider).toMatchObject({
    // issues, repos, and Other for the two without a group, as the table shows.
    operations: { total: 5, groups: 3, readOnly: 2, destructive: 1, blocked: 2 },
  })

  // A catalog with no groups at all is a flat list.
  responses['providers.listOperations'] = () => ({
    operations: [operation(readOnly)],
    nextPageToken: '',
  })
  expect((await load()).provider).toMatchObject({ operations: { groups: 0 } })

  // A provider's own group named Other is a group, not the flat-list fallback.
  responses['providers.listOperations'] = () => ({
    operations: [operation(readOnly, true, ['Other'])],
    nextPageToken: '',
  })
  expect((await load()).provider).toMatchObject({ operations: { groups: 1 } })
})
