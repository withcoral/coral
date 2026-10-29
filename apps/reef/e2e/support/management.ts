import { createClient } from '@connectrpc/connect'
import { createGrpcTransport } from '@connectrpc/connect-node'
import { inject } from 'vitest'

import {
  AgentRegistrationService,
  AgentService,
} from '../generated/coral/management/v1/agent_pb.ts'
import { TaskService } from '../generated/coral/management/v1/inspection_pb.ts'
import {
  ActivityService,
  type ActivityRunEvent,
} from '../generated/coral/management/v1/activity_pb.ts'
import { ProviderAccountService } from '../generated/coral/management/v1/account_pb.ts'
import { ToolPolicyService } from '../generated/coral/management/v1/policy_pb.ts'
import { WorkspaceService } from '../generated/coral/management/v1/workspace_pb.ts'
import { OrganizationMembershipService } from '../generated/coral/management/v1/organization_membership_pb.ts'
import { OrganizationWorkspaceService } from '../generated/coral/management/v1/organization_workspace_pb.ts'
import { WorkspaceMembershipService } from '../generated/coral/management/v1/membership_pb.ts'
import {
  ProviderService,
  ProviderSetupService,
} from '../generated/coral/management/v1/provider_pb.ts'

/** Every private management service Reel can reach. */
export const managementServices = [
  TaskService,
  ActivityService,
  WorkspaceService,
  OrganizationMembershipService,
  OrganizationWorkspaceService,
  WorkspaceMembershipService,
  AgentService,
  AgentRegistrationService,
  ProviderService,
  ProviderSetupService,
  ProviderAccountService,
  ToolPolicyService,
]

/**
 * Makes an organization owner a designated owner of a workspace they are not
 * in. An organization owner has no authority inside a workspace without this.
 */
export async function joinAsOwner(
  api: ReturnType<typeof managementClients>,
  workspace: string,
  userId: string,
) {
  await api.organizationWorkspaces.assignWorkspaceOwner({
    name: workspace,
    owner: `organizationMemberships/${userId}`,
    requestId: crypto.randomUUID(),
  })
}

/**
 * Resource-specific private management clients. On the authenticated target
 * pass an Actor's reef-audience bearer; on the local target the implicit
 * Organization Owner needs none.
 */
export function managementClients(accessToken?: string) {
  const transport = managementTransport(accessToken)
  return {
    tasks: createClient(TaskService, transport),
    activity: createClient(ActivityService, transport),
    workspaces: createClient(WorkspaceService, transport),
    organizationMemberships: createClient(OrganizationMembershipService, transport),
    organizationWorkspaces: createClient(OrganizationWorkspaceService, transport),
    memberships: createClient(WorkspaceMembershipService, transport),
    agents: createClient(AgentService, transport),
    registrations: createClient(AgentRegistrationService, transport),
    providers: createClient(ProviderService, transport),
    setups: createClient(ProviderSetupService, transport),
    accounts: createClient(ProviderAccountService, transport),
    policies: createClient(ToolPolicyService, transport),
  }
}
/** Reads complete retained history through the authorized human interface, not MCP output. */
export async function inspectRunEvents(api: ReturnType<typeof managementClients>, name: string) {
  const events: ActivityRunEvent[] = []
  let eventAfter = 0n
  for (;;) {
    const page = await api.activity.inspectRun({ name, eventAfter, pageSize: 100 })
    events.push(...page.events)
    if (!page.hasMoreEvents) return events
    eventAfter = page.nextEventAfter
  }
}

export function registrationClient(accessToken?: string) {
  return managementClients(accessToken).registrations
}
function managementTransport(accessToken?: string) {
  const { target } = inject('reel')
  if (!target.grpcURL) throw new Error('This target has no private gRPC endpoint')
  return createGrpcTransport({
    baseUrl: target.grpcURL,
    defaultTimeoutMs: 15_000,
    interceptors: [
      (next) => (request) => {
        request.header.set('coral-audience', 'reef')
        if (accessToken) request.header.set('authorization', `Bearer ${accessToken}`)
        return next(request)
      },
    ],
  })
}
