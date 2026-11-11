import type { MessageInitShape } from '@bufbuild/protobuf'
import { timestampFromDate } from '@bufbuild/protobuf/wkt'
import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import {
  UsageDataSchema,
  UsageHostKind,
  UsageSendBlock,
} from '@/generated/coral/management/v1/usage_data_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader } from './usage-data'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

// A report as Coral stores it: key order and spacing are Coral's, not Reef's.
const next = '{"v":1,"install":"3f9c8d2e-5b41-4c7a-9e06-1d7b2c4fa21e","day":{"date":"2026-11-09"}}'
const sent = '{"v":1,  "day":{"date":"2026-11-08"}}'
let usage: MessageInitShape<typeof UsageDataSchema>
const setSharing = vi.fn()

beforeEach(() => {
  vi.resetAllMocks()
  usage = {
    sharing: true,
    canChange: false,
    block: UsageSendBlock.NONE,
    kind: UsageHostKind.SERVER,
    nextReportJson: next,
  }
  coral.transport = fakeManagementTransport({
    usageData: {
      getUsageData: () => usage,
      setUsageDataSharing: (request) => {
        setSharing(request.sharing)
        return { ...usage, sharing: request.sharing }
      },
    },
  })
})

const origin = 'http://127.0.0.1:3000'
const url = `${origin}/usage-data`
const load = () => loader(authRouteTestArgs(new Request(url), {}, null))
const post = (fields: Record<string, string>) => {
  const form = new FormData()
  for (const [key, value] of Object.entries(fields)) form.set(key, value)
  return action(
    authRouteTestArgs(
      new Request(url, { method: 'POST', headers: { origin }, body: form }),
      {},
      null,
    ),
  )
}

// Catches: Reef re-serializing a report, so the page no longer shows what Coral sends.
it('passes both reports through byte for byte', async () => {
  usage.lastSent = { json: sent, sentTime: timestampFromDate(new Date('2026-11-09T00:12:00Z')) }
  const result = await load()
  expect(result.data.view).toEqual({
    choice: 'on',
    canChange: false,
    kind: 'server',
    next,
    lastSent: { at: '2026-11-09T00:12:00.000Z', json: sent },
  })
})

// Catches: a block Coral reports not reaching the page's banner.
it.each([
  [UsageSendBlock.DO_NOT_TRACK, { kind: 'environment', variable: 'DO_NOT_TRACK' }],
  [
    UsageSendBlock.CORAL_DISABLE_USAGE_DATA,
    { kind: 'environment', variable: 'CORAL_DISABLE_USAGE_DATA' },
  ],
  [UsageSendBlock.SOURCE_BUILD, { kind: 'build' }],
  [UsageSendBlock.UNSPECIFIED, { kind: 'unknown' }],
])('shows why this process never sends (%s)', async (block, expected) => {
  usage.block = block
  usage.kind = UsageHostKind.DESKTOP
  const view = (await load()).data.view
  expect(view.block).toEqual(expected)
  expect(view.kind).toBe('desktop')
})

// Catches: the page showing an owner's view as a member's, or "off" as "on".
it('shows sharing off and lets an owner change it', async () => {
  usage.sharing = false
  usage.canChange = true
  const view = (await load()).data.view
  expect(view.choice).toBe('off')
  expect(view.canChange).toBe(true)
})

// Catches: the switch sending Coral the opposite of what the owner chose.
it.each([
  ['off', false],
  ['on', true],
])('turns sharing %s', async (sharing, expected) => {
  const result = await post({ sharing })
  expect(result).toMatchObject({ data: { error: null } })
  expect(setSharing).toHaveBeenCalledExactlyOnceWith(expected)
})

// Catches: a member's refused change looking like it worked.
it('says why Coral refused the change', async () => {
  coral.transport = fakeManagementTransport({
    usageData: {
      setUsageDataSharing: () => {
        throw new ConnectError('permission denied', Code.PermissionDenied)
      },
    },
  })
  const result = await post({ sharing: 'off' })
  expect(result).toMatchObject({
    data: {
      error:
        'Coral could not change usage data sharing. You do not have permission for this action.',
    },
    init: { status: 403 },
  })
})

// Catches: a malformed form reaching Coral as a change.
it('rejects a form without a clear choice', async () => {
  await expect(post({ sharing: 'maybe' })).rejects.toMatchObject({ status: 400 })
  expect(setSharing).not.toHaveBeenCalled()
})
