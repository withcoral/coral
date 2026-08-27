import type { Meta, StoryObj } from '@storybook/react-vite'

import { createRoutesStub, useLoaderData } from 'react-router'
import { fn } from 'storybook/test'

import { addToast, ToastContainer } from '@/wax/components/toast'

import {
  WorkspaceUsers,
  type WorkspaceUserRole,
  type WorkspaceUsersActionData,
  type WorkspaceUsersData,
} from './workspace-users'

const MEMBERS: WorkspaceUsersData['members'] = [
  { displayName: 'Ada Lovelace', role: 'owner', userId: 'usr_ada' },
  { displayName: 'Grace Hopper', role: 'owner', userId: 'usr_grace' },
  { displayName: 'Lin Chen', role: 'member', userId: 'usr_lin' },
  { role: 'member', userId: 'usr_01JYB8X4N6' },
]

const AVAILABLE_USERS: NonNullable<WorkspaceUsersData['availableUsers']> = [
  { displayName: 'Katherine Johnson', userId: 'usr_katherine' },
  { displayName: 'Margaret Hamilton', userId: 'usr_margaret' },
  { userId: 'usr_01K0AVAILABLE' },
]

const DEFAULT_DATA: WorkspaceUsersData = {
  availableUsers: AVAILABLE_USERS,
  currentUserId: 'usr_ada',
  currentUserRole: 'owner',
  members: MEMBERS,
  workspaceName: 'analytics',
}

interface StoryArgs {
  action: (args: { request: Request }) => unknown
  data: WorkspaceUsersData
}

const meta: Meta<StoryArgs> = {
  args: {
    action: fn(async ({ request }: { request: Request }) => {
      const formData = await request.formData()
      const submittedIntent = formData.get('intent')
      const intent =
        submittedIntent === 'add' || submittedIntent === 'remove' ? submittedIntent : 'role'
      const result: WorkspaceUsersActionData = {
        intent,
        message:
          intent === 'add'
            ? 'Workspace users added.'
            : intent === 'remove'
              ? 'Workspace user removed.'
              : 'Workspace user updated.',
        status: 'success',
        userIds: formData.getAll('userId').map(String),
      }
      addToast('success', { title: result.message })
      return result
    }),
    data: DEFAULT_DATA,
  },
  component: WorkspaceUsers,
  parameters: { layout: 'fullscreen' },
  render: (args) => <WorkspaceUsersStory {...args} />,
  tags: ['autodocs'],
  title: 'Components/WorkspaceUsers',
}

export default meta
type Story = StoryObj<StoryArgs>

export const Default: Story = {}

/**
 * Adds that the server refuses. `usr_katherine` is accepted; Margaret Hamilton and
 * `usr_01K0AVAILABLE` are always refused, so one dialog covers every path:
 * pick all three for a partial add, the two refused for a whole-batch failure, and
 * Katherine alone for a clean one. A refused add keeps the dialog open, toasts the
 * reason per user, and leaves only the refused users selected for the retry.
 */
export const AddRefused: Story = {
  args: {
    action: fn(async ({ request }: { request: Request }) => {
      const formData = await request.formData()
      const userIds = formData.getAll('userId').map(String)
      const refused = userIds.filter((userId) => userId !== 'usr_katherine')
      const added = userIds.filter((userId) => userId === 'usr_katherine')

      if (formData.get('intent') !== 'add' || refused.length === 0) {
        const result: WorkspaceUsersActionData = {
          intent: 'add',
          message: 'Workspace users added.',
          status: 'success',
          userIds,
        }
        addToast('success', { title: result.message })
        return result
      }

      return {
        failures: refused.map((userId) => ({
          message: 'This user has no seat on the current plan.',
          userId,
        })),
        intent: 'add',
        message:
          added.length > 0
            ? `Added ${added.length} of ${userIds.length} users.`
            : 'Coral could not add these users.',
        status: added.length > 0 ? 'success' : 'error',
        userIds: added,
      } satisfies WorkspaceUsersActionData
    }),
    data: DEFAULT_DATA,
  },
}

export const NonOwner: Story = {
  args: {
    data: { ...DEFAULT_DATA, currentUserRole: 'member', members: [] },
  },
}

export const LoadError: Story = {
  args: {
    data: { ...DEFAULT_DATA, error: 'Coral could not load workspace users.', members: [] },
  },
}

function WorkspaceUsersStory({ action, data }: StoryArgs) {
  let storyData: WorkspaceUsersData = {
    ...data,
    availableUsers: [...data.availableUsers],
    members: [...data.members],
  }
  const RoutesStub = createRoutesStub([
    {
      action: async ({ request }) => {
        const formData = await request.clone().formData()
        const result = await action({ request })

        if (isSuccessfulAction(result)) {
          storyData = applySuccessfulAction(storyData, formData, result)
        }

        return result
      },
      Component: WorkspaceUsersStoryRoute,
      loader: () => storyData,
      path: '/workspaces/:workspaceId/users',
    },
  ])

  return (
    <>
      <RoutesStub initialEntries={[`/workspaces/${data.workspaceName}/users`]} />
      <ToastContainer />
    </>
  )
}

function WorkspaceUsersStoryRoute() {
  const data = useLoaderData() as WorkspaceUsersData
  return <WorkspaceUsers data={data} />
}

function isSuccessfulAction(result: unknown): result is WorkspaceUsersActionData {
  return (
    typeof result === 'object' &&
    result !== null &&
    'status' in result &&
    result.status === 'success'
  )
}

function applySuccessfulAction(
  data: WorkspaceUsersData,
  formData: FormData,
  result: WorkspaceUsersActionData,
): WorkspaceUsersData {
  if (result.intent === 'add') {
    const added = new Set(result.userIds)
    const candidates = data.availableUsers.filter((user) => added.has(user.userId))
    const role = formData.get('role') as WorkspaceUserRole

    return {
      ...data,
      availableUsers: data.availableUsers.filter((user) => !added.has(user.userId)),
      members: [...data.members, ...candidates.map((candidate) => ({ ...candidate, role }))],
    }
  }

  const userId = result.userIds[0] ?? ''

  if (result.intent === 'remove') {
    const member = data.members.find((user) => user.userId === userId)
    if (!member) return data

    return {
      ...data,
      availableUsers: [
        ...data.availableUsers,
        { displayName: member.displayName, userId: member.userId },
      ],
      currentUserRole: userId === data.currentUserId ? 'member' : data.currentUserRole,
      members: data.members.filter((user) => user.userId !== userId),
    }
  }

  const role = formData.get('role') as WorkspaceUserRole
  return {
    ...data,
    currentUserRole: userId === data.currentUserId ? role : data.currentUserRole,
    members: data.members.map((user) => (user.userId === userId ? { ...user, role } : user)),
  }
}
