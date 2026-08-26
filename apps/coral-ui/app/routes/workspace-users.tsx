import { create } from '@bufbuild/protobuf'
import { data, redirect } from 'react-router'

import type { Route } from './+types/workspace-users'

import { requestAuthContext } from '@/auth/server-context'
import {
  WorkspaceUsers,
  type WorkspaceUser,
  type WorkspaceUserCandidate,
  type WorkspaceUserRole,
  type WorkspaceUsersActionData,
  type WorkspaceUsersData,
} from '@/components/workspace-users'
import {
  AddWorkspaceMemberRequestSchema,
  ListWorkspaceMembersRequestSchema,
  RemoveWorkspaceMemberRequestSchema,
  WorkspaceRole,
  type WorkspaceMember,
  type WorkspaceMembership,
} from '@/generated/coral/v1/workspaces_pb'
import { GetCurrentUserRequestSchema, ListUsersRequestSchema } from '@/generated/coral/v1/users_pb'
import { userClientForRequest, workspaceClientForRequest } from '@/lib/coral-request.server'
import { errorMessage } from '@/lib/utils'
import { routePath } from '@/routing/routemap'
import { addToast } from '@/wax/components/toast'
import { workspaceFromParams } from '@/lib/workspace-routing'

export async function loader({
  context,
  params,
  request,
}: Route.LoaderArgs): Promise<WorkspaceUsersData> {
  const accessToken = context.get(requestAuthContext).accessToken
  const workspace = workspaceFromParams(params)
  const workspaceClient = workspaceClientForRequest(request, accessToken)
  const userClient = userClientForRequest(request, accessToken)

  const [currentUserResponse, workspacesResponse] = await Promise.all([
    userClient.getCurrentUser(create(GetCurrentUserRequestSchema, {}), { signal: request.signal }),
    workspaceClient.listWorkspaces({}, { signal: request.signal }),
  ])
  const currentUser = currentUserResponse.user
  if (!currentUser) throw new Error('Coral did not return the current user')

  const currentMembership = membershipForWorkspace(workspacesResponse.memberships, workspace.name)
  if (!currentMembership) {
    throw new Response('Workspace not found.', {
      status: 404,
      statusText: 'Workspace Not Found',
    })
  }

  const currentUserRole = toWorkspaceUserRole(currentMembership.role)
  const baseData = {
    availableUsers: [],
    currentUserId: currentUser.userId,
    currentUserRole,
    members: [],
    workspaceName: workspace.name,
  } satisfies WorkspaceUsersData

  if (currentUserRole !== 'owner') return baseData

  try {
    const [directory, roster] = await Promise.all([
      userClient.listUsers(create(ListUsersRequestSchema, {}), { signal: request.signal }),
      workspaceClient.listWorkspaceMembers(
        create(ListWorkspaceMembersRequestSchema, { workspace }),
        { signal: request.signal },
      ),
    ])

    return {
      ...baseData,
      availableUsers: directory.users.map(toWorkspaceUserCandidate),
      members: roster.members.map(toWorkspaceUser),
    }
  } catch (error) {
    return { ...baseData, error: errorMessage(error) }
  }
}

export async function action({ context, params, request }: Route.ActionArgs) {
  const formData = await request.formData()
  const intent = formIntent(formData)
  if (!intent) {
    return data(actionError('unsupported', [], 'Unsupported workspace user action.'), {
      status: 400,
    })
  }

  const userIds = formStrings(formData, 'userId')
  if (userIds.length === 0) return actionError(intent, [], 'Choose a user.')

  const workspace = workspaceFromParams(params)
  const accessToken = context.get(requestAuthContext).accessToken
  const workspaceClient = workspaceClientForRequest(request, accessToken)

  try {
    if (intent === 'remove') {
      const userId = userIds[0]
      const currentUserResponse = await userClientForRequest(request, accessToken).getCurrentUser(
        create(GetCurrentUserRequestSchema, {}),
        { signal: request.signal },
      )
      const currentUser = currentUserResponse.user
      if (!currentUser) throw new Error('Coral did not return the current user')

      await workspaceClient.removeWorkspaceMember(
        create(RemoveWorkspaceMemberRequestSchema, { userId, workspace }),
        { signal: request.signal },
      )
      return actionSuccess(intent, userId, 'Workspace user removed.', {
        removedCurrentUser: currentUser.userId === userId,
      })
    }

    const role = formRole(formData)
    if (!role) return actionError(intent, userIds, 'Choose a role.')

    if (intent === 'add') {
      const results = await Promise.allSettled(
        userIds.map((userId) =>
          workspaceClient.addWorkspaceMember(
            create(AddWorkspaceMemberRequestSchema, { role, userId, workspace }),
            { signal: request.signal },
          ),
        ),
      )
      const added = userIds.filter((_, index) => results[index].status === 'fulfilled')
      const failures = results.flatMap((result, index) =>
        result.status === 'rejected'
          ? [{ message: errorMessage(result.reason), userId: userIds[index] }]
          : [],
      )

      if (failures.length > 0) {
        return {
          failures,
          intent,
          message:
            added.length > 0
              ? `Added ${added.length} of ${userIds.length} users.`
              : 'Coral could not add these users.',
          status: added.length > 0 ? 'success' : 'error',
          userIds: added,
        } satisfies WorkspaceUsersActionData
      }

      return actionSuccess(intent, userIds, 'Workspace users added.')
    }

    await workspaceClient.addWorkspaceMember(
      create(AddWorkspaceMemberRequestSchema, { role, userId: userIds[0], workspace }),
      { signal: request.signal },
    )
    return actionSuccess(intent, userIds, 'Workspace user updated.')
  } catch (error) {
    return actionError(intent, userIds, errorMessage(error))
  }
}

export async function clientAction({ params, serverAction }: Route.ClientActionArgs) {
  const result = await serverAction()
  if (result.status !== 'success' || result.intent !== 'remove' || !result.removedCurrentUser) {
    return result
  }

  addToast('success', {
    description: 'You no longer have access to this workspace.',
    title: `Left ${params.workspaceId}`,
  })
  return redirect(routePath('home'))
}

export default function WorkspaceUsersRoute({ loaderData }: Route.ComponentProps) {
  return <WorkspaceUsers data={loaderData} />
}

function membershipForWorkspace(
  memberships: readonly WorkspaceMembership[],
  workspaceName: string,
): WorkspaceMembership | undefined {
  return memberships.find((membership) => membership.workspace?.name === workspaceName)
}

function toWorkspaceUser(member: WorkspaceMember): WorkspaceUser {
  return {
    displayName: member.displayName || undefined,
    role: toWorkspaceUserRole(member.role),
    userId: member.userId,
  }
}

function toWorkspaceUserCandidate(user: {
  displayName: string
  userId: string
}): WorkspaceUserCandidate {
  return {
    displayName: user.displayName || undefined,
    userId: user.userId,
  }
}

function toWorkspaceUserRole(role: WorkspaceRole): WorkspaceUserRole {
  if (role === WorkspaceRole.OWNER) return 'owner'
  if (role === WorkspaceRole.MEMBER) return 'member'
  throw new Error(`Coral returned an unsupported workspace role: ${role}`)
}

function formIntent(
  formData: FormData,
): Exclude<WorkspaceUsersActionData['intent'], 'unsupported'> | undefined {
  const intent = formData.get('intent')
  return intent === 'add' || intent === 'remove' || intent === 'role' ? intent : undefined
}

function formRole(formData: FormData): WorkspaceRole | undefined {
  const role = formData.get('role')
  if (role === 'owner') return WorkspaceRole.OWNER
  if (role === 'member') return WorkspaceRole.MEMBER
  return undefined
}

function formStrings(formData: FormData, key: string): string[] {
  return formData
    .getAll(key)
    .filter((value): value is string => typeof value === 'string' && value.length > 0)
}

function actionSuccess(
  intent: WorkspaceUsersActionData['intent'],
  userIds: string | ReadonlyArray<string>,
  message: string,
  extra?: Pick<WorkspaceUsersActionData, 'removedCurrentUser'>,
): WorkspaceUsersActionData {
  return {
    intent,
    message,
    status: 'success',
    userIds: typeof userIds === 'string' ? [userIds] : userIds,
    ...extra,
  }
}

function actionError(
  intent: WorkspaceUsersActionData['intent'],
  userIds: ReadonlyArray<string>,
  message: string,
): WorkspaceUsersActionData {
  return { intent, message, status: 'error', userIds }
}
