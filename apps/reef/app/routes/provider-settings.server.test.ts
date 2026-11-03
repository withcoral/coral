import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  CredentialRoute_Kind,
  ProviderBlocker,
  ProviderSetupInputKind,
  ProviderState,
} from '@/generated/coral/management/v1/provider_pb'
import {
  GetProviderAccountResponseSchema,
  ProviderAccountScope,
  ProviderAccountState,
} from '@/generated/coral/management/v1/account_pb'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { action, loader } from './provider-settings'

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
              const response = await respond(request)
              // As Coral answers: every field of the message, defaults included.
              return `${service}.${method}` === 'accounts.getProviderAccount'
                ? create(GetProviderAccountResponseSchema, response as object)
                : response
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
      url: 'https://reader:fixture-password@gitlab.example.com/api/v4/openapi.json?private_token=fixture-token',
      kind: ProviderSetupInputKind.OPENAPI,
    }),
    'accounts.getProviderAccount': () => ({
      account: { state: ProviderAccountState.READY },
      canConnect: true,
    }),
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
  ])
})

// Catches: members reaching the owner-only source read, or its metadata.
it('does not read the source for a member', async () => {
  signedInAs('member')
  const data = await load()
  expect(calls).not.toContain('providers.getProviderSource')
  expect(data.source).toBeUndefined()
})

// Catches: the Built-in Provider's settings reporting a failed source read.
// Coral defines its catalog, so it has no source to read.
it('does not read the source of the built-in provider', async () => {
  responses['providers.getProvider'] = () =>
    provider({
      displayName: 'Coral',
      namespace: '',
      builtin: true,
      credentialRoute: { kind: CredentialRoute_Kind.NONE },
    })
  signedInAs('owner')
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
  // The URL's password and secret query value never reach the page.
  expect(JSON.stringify(data)).not.toContain('fixture-password')
  expect(JSON.stringify(data)).not.toContain('fixture-token')
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
  expect(calls.toSorted()).toEqual(['providers.getProvider'])
})

it('reads no account when calls use none', async () => {
  responses['providers.getProvider'] = () =>
    provider({ credentialRoute: { kind: CredentialRoute_Kind.NONE } })
  signedInAs('member')
  await load()
  expect(calls.toSorted()).toEqual(['providers.getProvider'])
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

// Catches: a disabled provider, or one with no callable tool, looking
// the same as a ready one in settings.
it('says whether the provider is enabled and has tools ready', async () => {
  responses['providers.getProvider'] = () =>
    provider({
      state: ProviderState.DISABLED,
      blockers: [ProviderBlocker.PROVIDER_DISABLED, ProviderBlocker.NO_TOOLS_READY],
    })
  signedInAs('member')
  expect((await load()).provider).toMatchObject({
    enabled: false,
    toolsReady: false,
  })

  responses['providers.getProvider'] = () => provider()
  expect((await load()).provider).toMatchObject({ enabled: true, toolsReady: true })
})

// Catches: an uploaded icon losing to the recipe's logo, or a provider
// without one offering a removal.
it('shows the uploaded icon first and lets an owner remove only that', async () => {
  signedInAs('member')
  // GitLab's recipe logo stands for a provider that keeps its namespace.
  const recipe = (await load()).provider as { icons?: { light: string }; uploadedIcon: boolean }
  expect(recipe.uploadedIcon).toBe(false)
  expect(recipe.icons?.light).not.toContain('/icon?v=')

  responses['providers.getProvider'] = () => provider({ iconDigest: 'digest-1' })
  expect((await load()).provider).toMatchObject({
    uploadedIcon: true,
    icons: { light: `/${name}/icon?v=digest-1` },
  })
})

/** Posts the icon form as a same-origin browser would, with `icon` as the chosen file. */
async function post(intent: string, icon?: File) {
  const form = new FormData()
  form.set('intent', intent)
  form.set('provider', providerId)
  form.set('etag', 'etag-1')
  if (icon) form.set('icon', icon)
  const origin = 'http://127.0.0.1:3000'
  const result = (await action(
    authRouteTestArgs(
      new Request(`${origin}/workspaces/${workspaceId}/providers/${providerId}/settings`, {
        method: 'POST',
        headers: { origin },
        body: form,
      }),
      { workspace: workspaceId, provider: providerId },
      null,
    ) as never,
  )) as unknown as { data: { error: string | null; message?: string }; init?: ResponseInit }
  return { ...result.data, status: result.init?.status ?? 200 }
}

const png = (size: number) => new File([new Uint8Array(size)], 'logo.png', { type: 'image/png' })

describe('icon changes', () => {
  const sent: Record<string, unknown>[] = []
  beforeEach(() => {
    sent.length = 0
    for (const method of ['setProviderIcon', 'deleteProviderIcon'])
      responses[`providers.${method}`] = (request) => {
        sent.push({ method, ...(request as object) })
        return provider()
      }
    signedInAs('owner')
  })

  // Catches: an empty or oversized file reaching Coral, which would answer
  // with a generic error instead of the rule the reader must follow.
  it.each([
    ['no file', undefined, 'Choose an icon file.'],
    ['an empty file', png(0), 'Choose an icon file.'],
    ['a file over 100 KiB', png(100 * 1024 + 1), 'This file is too large.'],
  ])('refuses %s before it calls Coral', async (_, icon, error) => {
    expect(await post('setProviderIcon', icon)).toMatchObject({
      status: 400,
      error: `${error} Use a PNG, JPEG, WebP or SVG file of 100 KiB or less.`,
    })
    expect(sent).toEqual([])
  })

  // Catches: the file sent as text or without the etag it was chosen against.
  it('sends the file bytes with the etag', async () => {
    const bytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 1, 2, 3])
    expect(
      await post('setProviderIcon', new File([bytes], 'logo.png', { type: 'image/png' })),
    ).toMatchObject({ error: null, message: 'Icon saved.' })
    expect(sent).toEqual([{ method: 'setProviderIcon', name, etag: 'etag-1', content: bytes }])
  })

  // Catches: Coral refusing the image shown as a generic form error.
  it('explains an image Coral refuses', async () => {
    responses['providers.setProviderIcon'] = () => {
      throw new ConnectError('INVALID_PROVIDER_INPUT', Code.InvalidArgument)
    }
    expect(await post('setProviderIcon', png(10))).toMatchObject({
      status: 400,
      error:
        'Coral cannot use this file as an icon. Use a PNG, JPEG, WebP or SVG file of 100 KiB or less.',
    })
  })

  // Catches: another invalid argument, such as a malformed etag, blamed on the file.
  it('does not blame the file for another invalid argument', async () => {
    responses['providers.setProviderIcon'] = () => {
      throw new ConnectError('INVALID_ARGUMENT', Code.InvalidArgument)
    }
    const result = await post('setProviderIcon', png(10))
    expect(result).toMatchObject({ status: 400 })
    expect(result.error).not.toContain('Coral cannot use this file as an icon.')
  })

  it('removes the icon with the etag', async () => {
    expect(await post('deleteProviderIcon')).toMatchObject({
      error: null,
      message: 'Icon removed.',
    })
    expect(sent).toEqual([{ method: 'deleteProviderIcon', name, etag: 'etag-1' }])
  })
})
