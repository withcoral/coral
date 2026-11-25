import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import {
  SubAgentModelProtocol,
  SubAgentModelTestOutcome,
} from '@/generated/coral/management/v1/sub_agent_model_pb'
import { coralRefusal, fakeManagementTransport } from '@/lib/management.test-support'
import { action } from './organization-sub-agent-test'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

const key = 'sk-secret'
let refuse: ConnectError | null
let reply: { outcome: SubAgentModelTestOutcome; httpStatus?: number }
/** Each test that reached Coral, and how long Reef waits for it. */
let sent: object[]

beforeEach(() => {
  refuse = null
  reply = { outcome: SubAgentModelTestOutcome.ANSWERED }
  sent = []
  coral.transport = fakeManagementTransport({
    subAgentModel: {
      testSubAgentModel: ({ settings, apiKey }, { timeoutMs }) => {
        sent.push({ ...settings, $typeName: undefined, apiKey, waits: (timeoutMs() ?? 0) > 45_000 })
        if (refuse) throw refuse
        return reply
      },
    },
  })
})

const origin = 'http://127.0.0.1:3000'
const form = {
  intent: 'save',
  protocol: 'anthropic',
  baseUrl: 'https://api.example.com/v1',
  model: 'large-1',
  keyChange: 'replace',
  apiKey: key,
  contextWindow: '',
  requestArgs: '',
}
const post = (fields: Record<string, string> = {}) => {
  const body = new FormData()
  for (const [name, value] of Object.entries({ ...form, ...fields })) body.set(name, value)
  return action(
    authRouteTestArgs(
      new Request(`${origin}/organization/sub-agent/test`, {
        method: 'POST',
        headers: { origin },
        body,
      }),
      {},
      null,
    ),
  )
}

// Catches: a test with another key than a save would keep, or with one that the person cleared,
// and a wait that ends before Coral's own.
it.each([
  ['keep', key, undefined],
  ['replace', key, key],
  ['clear', key, ''],
  ['optional', key, key],
  ['optional', '', ''],
])(
  'sends what is in the form, with the key choice %s of "%s"',
  async (keyChange, typed, apiKey) => {
    await post({ keyChange, apiKey: typed, contextWindow: '48,000', requestArgs: '{"top_k":1}' })
    expect(sent).toEqual([
      {
        protocol: SubAgentModelProtocol.ANTHROPIC,
        baseUrl: 'https://api.example.com/v1',
        model: 'large-1',
        contextWindow: 48_000,
        requestArgsJson: '{"top_k":1}',
        apiKey,
        waits: true,
      },
    ])
  },
)

const { ANSWERED, UNREACHABLE, REFUSED, FORBIDDEN, UNREADABLE } = SubAgentModelTestOutcome

// Catches: an outcome shown as another one, a refusal without its status, one that this Reef does
// not know shown as an answer, and an answer that holds the key or that the page cannot read.
it.each([
  [{ outcome: ANSWERED }, { outcome: 'answered' }],
  [{ outcome: UNREACHABLE }, { outcome: 'unreachable' }],
  [
    { outcome: REFUSED, httpStatus: 401 },
    { outcome: 'refused', status: 401 },
  ],
  [{ outcome: FORBIDDEN }, { outcome: 'forbidden' }],
  [{ outcome: UNREADABLE }, { outcome: 'unreadable' }],
  [{ outcome: 99 as SubAgentModelTestOutcome }, { outcome: 'unknown' }],
])('answers %j as %j', async (from, expected) => {
  reply = from
  const response = await post()
  expect(response.headers.get('Content-Type')).toBe('text/plain; charset=utf-8')
  expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  expect(JSON.parse(await response.text())).toEqual(expected)
})

// Catches: a refused setting that is not named, so the page cannot show it at its field, one that
// Reef can refuse itself sent to Coral, and a member's test that says it ran.
it.each([
  [
    coralRefusal(Code.InvalidArgument, 'INVALID_MODEL'),
    {},
    400,
    { outcome: 'invalid', field: 'model' },
  ],
  [
    coralRefusal(Code.InvalidArgument, 'DESTINATION_NOT_PERMITTED'),
    {},
    400,
    { outcome: 'invalid', field: 'baseUrl' },
  ],
  [null, { contextWindow: 'large' }, 400, { outcome: 'invalid', field: 'contextWindow' }],
  [null, { apiKey: '' }, 400, { outcome: 'invalid', field: 'apiKey' }],
  [
    new ConnectError('permission denied', Code.PermissionDenied),
    {},
    403,
    { outcome: 'failed', message: 'You do not have permission for this action.' },
  ],
  [
    new ConnectError('late', Code.DeadlineExceeded),
    {},
    503,
    { outcome: 'failed', message: 'Coral is not available.' },
  ],
])('says what was refused', async (refusal, fields, status, expected) => {
  refuse = refusal
  const response = await post(fields)
  expect(response.status).toBe(status)
  const text = await response.text()
  expect(JSON.parse(text)).toMatchObject(expected)
  expect(text).not.toContain(key)
  expect(sent).toHaveLength(refusal ? 1 : 0)
})

// Catches: a test that another site's page starts with the person's session.
it('refuses a form of another origin', async () => {
  const request = new Request(`${origin}/organization/sub-agent/test`, {
    method: 'POST',
    headers: { origin: 'https://elsewhere.example' },
    body: new FormData(),
  })
  await expect(action(authRouteTestArgs(request, {}, null))).rejects.toMatchObject({ status: 403 })
  expect(sent).toEqual([])
})
