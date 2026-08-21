import type { Meta, StoryObj } from '@storybook/react-vite'

import { createRoutesStub, useLoaderData } from 'react-router'
import { expect, fn, waitFor, within } from 'storybook/test'

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
  retryData?: WorkspaceUsersData
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
    retryData: DEFAULT_DATA,
  },
  play: async ({ canvasElement, userEvent }) => {
    const canvas = within(canvasElement)
    const retry = await canvas.findByRole('button', { name: 'Retry' })

    await expect(canvas.getByRole('button', { name: 'Add user' })).toBeDisabled()
    await userEvent.click(retry)
    await expect(canvas.getByRole('button', { name: 'Retrying…' })).toBeDisabled()

    await waitFor(() => expect(canvas.queryByRole('alert')).not.toBeInTheDocument())
    await expect(canvas.getByText('Ada Lovelace (you)')).toBeInTheDocument()
    await expect(canvas.getByRole('button', { name: 'Add user' })).toBeEnabled()
  },
}

export const RemovalRestoresFocusToHeading: Story = {
  play: async ({ canvasElement, userEvent }) => {
    const canvas = within(canvasElement)
    const document = within(canvasElement.ownerDocument.body)

    await userEvent.click(await canvas.findByRole('button', { name: 'Remove Lin Chen' }))
    await userEvent.click(await document.findByRole('button', { name: 'Remove user' }))

    await waitFor(() => expect(canvas.queryByText('Lin Chen')).not.toBeInTheDocument())
    await waitFor(() => expect(canvas.getByRole('heading', { name: 'Users' })).toHaveFocus())
  },
}

export const SuccessfulSelfDemotionRestoresFocusToHeading: Story = {
  play: async ({ canvasElement, userEvent }) => {
    const canvas = within(canvasElement)
    const document = within(canvasElement.ownerDocument.body)

    await userEvent.click(
      await canvas.findByRole('button', { name: 'Role for Ada Lovelace: Owner' }),
    )
    await userEvent.click(await document.findByRole('menuitemradio', { name: 'Member' }))
    await userEvent.click(await document.findByRole('button', { name: 'Change my role' }))

    await waitFor(() => expect(canvas.getByText('Owner access required')).toBeInTheDocument())
    await waitFor(() => expect(canvas.getByRole('heading', { name: 'Users' })).toHaveFocus())
  },
}

export const FailedSelfDemotionRestoresFocusToRole: Story = {
  args: {
    action: fn(async ({ request }: { request: Request }) => {
      const formData = await request.formData()
      return {
        intent: 'role' as const,
        message: 'Could not update workspace user.',
        status: 'error' as const,
        userId: String(formData.get('userId')),
      }
    }),
  },
  play: async ({ canvasElement, userEvent }) => {
    const canvas = within(canvasElement)
    const document = within(canvasElement.ownerDocument.body)
    const roleButton = await canvas.findByRole('button', {
      name: 'Role for Ada Lovelace: Owner',
    })

    await userEvent.click(roleButton)
    await userEvent.click(await document.findByRole('menuitemradio', { name: 'Member' }))
    await userEvent.click(await document.findByRole('button', { name: 'Change my role' }))

    await waitFor(() => expect(canvas.getByRole('alert')).toHaveTextContent('Could not update'))
    await waitFor(() => expect(roleButton).toHaveFocus())
  },
}

function WorkspaceUsersStory({ action, data, retryData }: StoryArgs) {
  let storyData: WorkspaceUsersData = {
    ...data,
    availableUsers: [...data.availableUsers],
    members: [...data.members],
  }
  let loadCount = 0
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
      loader: async () => {
        loadCount += 1
        if (loadCount > 1 && retryData) {
          await new Promise((resolve) => setTimeout(resolve, 100))
          storyData = retryData
        }
        return storyData
      },
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
  const userId = result.userId

  if (result.intent === 'add') {
    const candidate = data.availableUsers.find((user) => user.userId === userId)
    if (!candidate) return data

    return {
      ...data,
      availableUsers: data.availableUsers.filter((user) => user.userId !== userId),
      members: [...data.members, { ...candidate, role: formData.get('role') as WorkspaceUserRole }],
    }
  }

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
