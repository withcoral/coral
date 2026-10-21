import { expect, it } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { action } from './activity-summary'

const workspace = '00000000-0000-4000-8000-000000000001'

function post(body: string, origin: string | null = 'http://reef.test') {
  return new Request('http://reef.test/activity/summary', {
    method: 'POST',
    headers: origin ? { origin } : undefined,
    body,
  })
}

it.each([null, 'https://other.test'])(
  'rejects missing or foreign origins before allocating a summary stream',
  async (origin) => {
    await expect(
      action(authRouteTestArgs(post(JSON.stringify({ workspace, days: 7 }), origin), {})),
    ).rejects.toMatchObject({ status: 403 })
  },
)
it('rejects non-POST requests', async () => {
  await expect(
    action(authRouteTestArgs(new Request('http://reef.test/activity/summary'), {})),
  ).rejects.toMatchObject({ status: 405 })
})
it.each([
  ['malformed JSON', '{', 400],
  ['a period the page does not offer', JSON.stringify({ workspace, days: 3 }), 400],
  ['no period', JSON.stringify({ workspace }), 400],
  ['no workspace', JSON.stringify({ days: 7 }), 400],
  ['an oversized body', JSON.stringify({ workspace, days: 7, pad: 'x'.repeat(2048) }), 400],
  ['a malformed workspace ID', JSON.stringify({ workspace: '../agents', days: 7 }), 404],
])('rejects %s before reading Coral', async (_, body, status) => {
  await expect(action(authRouteTestArgs(post(body), {}))).rejects.toMatchObject({ status })
})
