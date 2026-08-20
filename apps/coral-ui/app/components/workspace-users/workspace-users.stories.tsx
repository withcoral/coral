import type { Meta, StoryObj } from '@storybook/react-vite'

import { createRoutesStub, useLoaderData } from 'react-router'
import { fn } from 'storybook/test'

import { addToast, ToastContainer } from '@/wax/components/toast'

import { WorkspaceUsers, type WorkspaceUsersData } from './workspace-users'

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
      const result = {
        intent,
        message:
          intent === 'add'
            ? 'Workspace user added.'
            : intent === 'remove'
              ? 'Workspace user removed.'
              : 'Workspace user updated.',
        status: 'success' as const,
        userId: String(formData.get('userId')),
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
  const RoutesStub = createRoutesStub([
    {
      action,
      Component: WorkspaceUsersStoryRoute,
      loader: () => data,
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
