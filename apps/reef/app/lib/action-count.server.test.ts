import { createClient, createRouterTransport, Code, ConnectError } from '@connectrpc/connect'
import { expect, it } from 'vitest'
import { ActivityService } from '@/generated/coral/management/v1/activity_pb'
import { loadActionCounts } from './action-count.server'

const finance = 'workspaces/00000000-0000-4000-8000-000000000001'
const research = 'workspaces/00000000-0000-4000-8000-000000000002'

it('reads each workspace count separately and keys it by workspace path', async () => {
  const activity = createClient(
    ActivityService,
    createRouterTransport(({ service }) => {
      service(ActivityService, {
        countRequiredActions: ({ workspace }) => ({
          actionCount: workspace === finance ? 9007199254740993n : 0n,
        }),
      })
    }),
  )
  expect(await loadActionCounts({ activity }, [{ name: finance }, { name: research }])).toEqual({
    [`/${finance}`]: '9007199254740993',
    [`/${research}`]: '0',
  })
})
function failing(code: Code) {
  return createClient(
    ActivityService,
    createRouterTransport(({ service }) => {
      service(ActivityService, {
        countRequiredActions: ({ workspace }) => {
          if (workspace === finance) throw new ConnectError('private error', code)
          return { actionCount: 2n }
        },
      })
    }),
  )
}
it.each([Code.NotFound, Code.PermissionDenied, Code.Unavailable])(
  'does not present an unavailable count as zero or hide the other counts (%s)',
  async (code) => {
    const activity = failing(code)
    expect(await loadActionCounts({ activity }, [{ name: finance }, { name: research }])).toEqual({
      [`/${finance}`]: null,
      [`/${research}`]: '2',
    })
  },
)
// The count stream must see a rejected session to end as expired, not show "?".
it('rejects when Coral rejects the session', async () => {
  const activity = failing(Code.Unauthenticated)
  await expect(
    loadActionCounts({ activity }, [{ name: finance }, { name: research }]),
  ).rejects.toMatchObject({ code: Code.Unauthenticated })
})
