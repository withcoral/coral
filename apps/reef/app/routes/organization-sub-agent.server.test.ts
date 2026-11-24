import type { MessageInitShape } from '@bufbuild/protobuf'
import { timestampFromDate } from '@bufbuild/protobuf/wkt'
import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import {
  type CreateSubAgentModelRequest,
  SubAgentModelProtocol,
  type SubAgentModelSchema,
  type SubAgentModelSettings,
  type UpdateSubAgentModelRequest,
} from '@/generated/coral/management/v1/sub_agent_model_pb'
import { coralRefusal, fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader } from './organization-sub-agent'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

const etag = '11111111-1111-4111-8111-111111111111'
const ada = { user: 'users/ada', displayName: 'Ada Lovelace', email: 'ada@example.com' }
const saved = {
  settings: {
    protocol: SubAgentModelProtocol.ANTHROPIC,
    baseUrl: 'https://api.example.com/v1',
    model: 'large-1',
    contextWindow: 200_000,
    // The second value does not fit a JavaScript number.
    requestArgsJson: '{"max_tokens":4096,"top_k":12345678901234567890}',
  },
  hasApiKey: true,
  changedBy: ada.user,
  changeTime: timestampFromDate(new Date('2026-11-20T14:03:00Z')),
  etag,
} satisfies MessageInitShape<typeof SubAgentModelSchema>

let model: MessageInitShape<typeof SubAgentModelSchema> | undefined
let refuse: ConnectError | null
/** What reached Coral, in order. */
let sent: {
  method: string
  settings?: Partial<SubAgentModelSettings>
  etag?: string
  apiKey?: string
}[]

beforeEach(() => {
  model = saved
  refuse = null
  sent = []
  const answer = <T>(value: () => T) => {
    if (refuse) throw refuse
    return value()
  }
  const change = (
    method: string,
    { settings, apiKey, ...rest }: CreateSubAgentModelRequest | UpdateSubAgentModelRequest,
  ) =>
    answer(() => {
      sent.push({
        method,
        settings: { ...settings, $typeName: undefined },
        etag: 'etag' in rest ? rest.etag : undefined,
        apiKey,
      })
      return saved
    })
  coral.transport = fakeManagementTransport({
    subAgentModel: {
      getSubAgentModel: () => answer(() => ({ model })),
      createSubAgentModel: (request) => change('create', request),
      updateSubAgentModel: (request) => change('update', request),
      deleteSubAgentModel: (request) =>
        answer(() => {
          sent.push({ method: 'delete', etag: request.etag })
          return {}
        }),
    },
    organizationMemberships: {
      // The person is on the second page, and is disabled.
      listOrganizationMemberships: ({ pageToken }) =>
        pageToken === 'more'
          ? { memberships: [{ name: 'organizationMemberships/ada', profile: ada, enabled: false }] }
          : {
              memberships: [
                {
                  name: 'organizationMemberships/grace',
                  profile: { user: 'users/grace', displayName: 'Grace Hopper' },
                  enabled: true,
                },
              ],
              nextPageToken: 'more',
            },
    },
  })
})

const origin = 'http://127.0.0.1:3000'
const url = `${origin}/organization/sub-agent`
const load = () => loader(authRouteTestArgs(new Request(url), {}, null))
const form = {
  intent: 'save',
  protocol: 'openai_responses',
  baseUrl: 'https://models.example.com/v1',
  model: 'small-2',
  keyChange: 'keep',
  contextWindow: '',
  requestArgs: '',
}
const post = (fields: Record<string, string>) => {
  const body = new FormData()
  for (const [key, value] of Object.entries(fields)) body.set(key, value)
  return action(
    authRouteTestArgs(new Request(url, { method: 'POST', headers: { origin }, body }), {}, null),
  )
}
/** The result of a save that Coral or Reef refused. */
const refused = async (fields: Record<string, string>) => {
  const result = await post(fields)
  if (result instanceof Response) throw new Error('expected a refusal, got a redirect')
  return result
}

// Catches: a setting lost or rounded on its way to the form, and a person who is disabled, or
// on a later page of members, shown as one who left.
it('reads the saved model and who changed it', async () => {
  expect((await load()).data.model).toEqual({
    values: {
      protocol: 'anthropic',
      baseUrl: 'https://api.example.com/v1',
      model: 'large-1',
      contextWindow: '200000',
      requestArgs: '{\n  "max_tokens": 4096,\n  "top_k": 12345678901234567890\n}',
    },
    hasApiKey: true,
    changedBy: { user: ada.user, name: ada.displayName, email: ada.email, local: false },
    changedAt: '2026-11-20T14:03:00.000Z',
    etag,
  })
})

// Catches: a page that does not open because the person who changed the model has left.
it('reads a model that a person who left changed', async () => {
  model = { ...saved, changedBy: 'users/left' }
  expect((await load()).data.model).toMatchObject({ changedBy: null, etag })
})

// Catches: the absence of a model, or of its optional settings, failing the page.
it('reads no model, and a model with no optional settings', async () => {
  model = undefined
  expect((await load()).data.model).toBeNull()
  const { contextWindow: _, ...required } = saved.settings
  model = { ...saved, settings: { ...required, requestArgsJson: '' }, hasApiKey: false }
  expect((await load()).data.model).toMatchObject({
    values: { contextWindow: '', requestArgs: '' },
    hasApiKey: false,
  })
})

// Catches: a protocol from a newer Coral shown as another one, which a save would then set.
it('does not open on a protocol this Reef does not know', async () => {
  model = { ...saved, settings: { ...saved.settings, protocol: 99 as SubAgentModelProtocol } }
  await expect(load()).rejects.toMatchObject({ status: 503 })
})

// Catches: a member who reaches the URL getting anything but Coral's refusal.
it('passes on the refusal when the caller is not an organization owner', async () => {
  refuse = new ConnectError('permission denied', Code.PermissionDenied)
  await expect(load()).rejects.toMatchObject({ status: 403 })
  expect(await refused({ ...form, etag })).toMatchObject({
    data: { message: 'You do not have permission for this action.' },
    init: { status: 403 },
  })
})

// Catches: a save that keeps a key it should replace or clear, clears one it should keep, or
// creates a model in place of changing the one with this etag.
it.each([
  ['a kept key', { etag, keyChange: 'keep', apiKey: 'typed' }, 'update', undefined],
  ['a replaced key', { etag, keyChange: 'replace', apiKey: 'sk-new' }, 'update', 'sk-new'],
  ['a cleared key', { etag, keyChange: 'clear' }, 'update', ''],
  ['a first key', { keyChange: 'replace', apiKey: 'sk-new' }, 'create', 'sk-new'],
  ['no first key', { keyChange: 'clear' }, 'create', undefined],
])('saves %s', async (_, fields, method, apiKey) => {
  const result = await post({ ...form, ...fields })
  expect(result).toBeInstanceOf(Response)
  expect((result as Response).headers.get('location')).toBe('/organization/sub-agent?saved=1')
  expect(sent).toEqual([
    {
      method,
      settings: {
        protocol: SubAgentModelProtocol.OPENAI_RESPONSES,
        baseUrl: 'https://models.example.com/v1',
        model: 'small-2',
        requestArgsJson: '',
      },
      etag: 'etag' in fields ? etag : undefined,
      apiKey,
    },
  ])
})

// Catches: an empty window sent as zero, or one with the commas that the page shows refused.
it.each([
  ['', undefined],
  [' 48,000 ', 48_000],
  ['4294967295', 4_294_967_295],
])('sends a context window of "%s" as %s', async (contextWindow, expected) => {
  await post({ ...form, contextWindow })
  expect(sent).toHaveLength(1)
  expect(sent[0].settings?.contextWindow).toBe(expected)
})

const typed = {
  protocol: 'openai_responses',
  baseUrl: 'https://models.example.com/v1',
  model: 'small-2',
  contextWindow: '9',
  requestArgs: '{\n  "stream": true\n}',
}

// Catches: a refusal shown away from its field, one that loses what the person typed, and one
// that sends the API key back to the browser.
it.each([
  ['INVALID_BASE_URL', 'baseUrl', 'Enter an http or https URL.'],
  ['DESTINATION_NOT_PERMITTED', 'baseUrl', 'This deployment does not permit this URL.'],
  ['INVALID_MODEL', 'model', 'Enter a model name of 200 characters or fewer.'],
  ['INVALID_API_KEY', 'apiKey', 'Enter an API key without line breaks'],
  ['INVALID_CONTEXT_WINDOW', 'contextWindow', 'Enter a whole number, 48,000 or more.'],
  ['INVALID_REQUEST_ARGS', 'requestArgs', 'Enter a JSON object of 16 KB or less.'],
  ['REQUEST_ARG_NOT_ALLOWED', 'requestArgs', 'Remove each name that is not in the list below.'],
])('shows %s at its field and keeps what the person typed', async (reason, field, message) => {
  refuse = coralRefusal(Code.InvalidArgument, reason)
  const result = await refused({
    ...form,
    ...typed,
    // As a form submission sends a text area.
    requestArgs: typed.requestArgs.replaceAll('\n', '\r\n'),
    etag,
    keyChange: 'replace',
    apiKey: 'sk-secret',
  })
  expect(result).toMatchObject({
    data: { field, values: typed, keyChange: 'replace' },
    init: { status: 400 },
  })
  expect(result.data).toHaveProperty('message', expect.stringContaining(message))
  expect(JSON.stringify(result)).not.toContain('sk-secret')
})

// Catches: a save that lost to another person's change shown as a form error, or one that
// loses what the person typed.
it.each([
  ['a stale save', { etag }, coralRefusal(Code.Aborted, 'ETAG_MISMATCH'), 'changed the model'],
  [
    'a second create',
    {},
    coralRefusal(Code.AlreadyExists, 'SUB_AGENT_MODEL_ALREADY_SET'),
    'set the model',
  ],
  [
    'a save of a removed model',
    { etag },
    new ConnectError('not found', Code.NotFound),
    'removed the model',
  ],
])('says that %s lost to another person', async (_, fields, refusal, message) => {
  refuse = refusal
  const result = await refused({
    ...form,
    ...typed,
    ...fields,
    keyChange: 'replace',
    apiKey: 'sk-secret',
  })
  expect(result.data).toMatchObject({ conflict: true, values: typed, keyChange: 'replace' })
  expect(result.data).not.toHaveProperty('field')
  expect(result.data).toHaveProperty(
    'message',
    expect.stringContaining(`Another person ${message} after you opened this page.`),
  )
  expect(JSON.stringify(result)).not.toContain('sk-secret')
})

// Catches: a window that the wire cannot hold failing the whole page, and an empty new key
// clearing the saved one.
it.each([
  ['contextWindow', { contextWindow: 'large' }],
  ['contextWindow', { contextWindow: '-1' }],
  ['contextWindow', { contextWindow: '4294967296' }],
  ['apiKey', { keyChange: 'replace', apiKey: '' }],
])('refuses a %s of %j before Coral', async (field, fields) => {
  expect(await refused({ ...form, etag, ...fields })).toMatchObject({
    data: { field, values: { baseUrl: form.baseUrl } },
    init: { status: 400 },
  })
  expect(sent).toEqual([])
})

// Catches: a malformed form reaching Coral as a change.
it.each([{ protocol: 'other' }, { keyChange: 'other' }, { intent: 'other' }])(
  'rejects a form with %j',
  async (fields) => {
    await expect(post({ ...form, etag, ...fields })).rejects.toMatchObject({ status: 400 })
    expect(sent).toEqual([])
  },
)

// Catches: a removal of another model than the one on the page, and a refused removal that
// looks like it worked.
it('removes the model with the etag on the page, and says why when it cannot', async () => {
  const removed = await post({ intent: 'remove', etag })
  expect(removed).toBeInstanceOf(Response)
  expect((removed as Response).headers.get('location')).toBe('/organization/sub-agent?removed=1')
  expect(sent).toEqual([{ method: 'delete', etag }])
  refuse = coralRefusal(Code.Aborted, 'ETAG_MISMATCH')
  expect(await post({ intent: 'remove', etag })).toMatchObject({
    data: { error: expect.stringMatching(/^Coral could not remove the model\. .*Reload the page/) },
    init: { status: 409 },
  })
})
