import type { DescService } from '@bufbuild/protobuf'
import { BinaryWriter, WireType } from '@bufbuild/protobuf/wire'
import {
  ConnectError,
  createClient,
  createRouterTransport,
  type Code,
  type ServiceImpl,
  type Transport,
} from '@connectrpc/connect'

import { ProviderAccountService } from '@/generated/coral/management/v1/account_pb'
import { ActivityService } from '@/generated/coral/management/v1/activity_pb'
import { AgentRegistrationService, AgentService } from '@/generated/coral/management/v1/agent_pb'
import { WorkspaceMembershipService } from '@/generated/coral/management/v1/membership_pb'
import { OrganizationMembershipService } from '@/generated/coral/management/v1/organization_membership_pb'
import { OrganizationWorkspaceService } from '@/generated/coral/management/v1/organization_workspace_pb'
import { ToolPolicyService } from '@/generated/coral/management/v1/policy_pb'
import { ProviderService, ProviderSetupService } from '@/generated/coral/management/v1/provider_pb'
import { ReleaseService } from '@/generated/coral/management/v1/release_pb'
import { PrivateToolService } from '@/generated/coral/management/v1/private_tool_pb'
import { SharedToolService } from '@/generated/coral/management/v1/shared_tool_pb'
import { UsageDataService } from '@/generated/coral/management/v1/usage_data_pb'
import { WorkspaceService } from '@/generated/coral/management/v1/workspace_pb'

import type { ManagementApi } from './management.server'

// Checked against `ManagementApi`, so a client added to `management()` must be named here too.
const services = {
  workspaces: WorkspaceService,
  activity: ActivityService,
  organizationMemberships: OrganizationMembershipService,
  organizationWorkspaces: OrganizationWorkspaceService,
  memberships: WorkspaceMembershipService,
  agents: AgentService,
  registrations: AgentRegistrationService,
  providers: ProviderService,
  setups: ProviderSetupService,
  accounts: ProviderAccountService,
  policies: ToolPolicyService,
  privateTools: PrivateToolService,
  sharedTools: SharedToolService,
  releases: ReleaseService,
  usageData: UsageDataService,
} satisfies Record<keyof ManagementApi, DescService>

type Services = typeof services

/** Handlers for the Coral methods a test uses, keyed like the `management()` clients. */
export type ManagementHandlers = {
  [Client in keyof Services]?: Partial<ServiceImpl<Services[Client]>>
}

/**
 * An in-memory Coral that serves `handlers`. Requests and responses cross the
 * generated message schemas; a method without a handler fails as unimplemented.
 * Handlers receive `(request, context)`.
 */
export function fakeManagementTransport(handlers: ManagementHandlers): Transport {
  return createRouterTransport(({ service }) => {
    for (const [client, impl] of Object.entries(handlers)) {
      service(services[client as keyof Services], impl as Partial<ServiceImpl<DescService>>)
    }
  })
}

/** The `management()` clients over `fakeManagementTransport(handlers)`. */
export function fakeManagement(handlers: ManagementHandlers): ManagementApi {
  const transport = fakeManagementTransport(handlers)
  return Object.fromEntries(
    Object.entries(services).map(([client, service]) => [client, createClient(service, transport)]),
  ) as ManagementApi
}

/**
 * A refusal as Coral sends it: the reason is the status message, and
 * `metadata` rides in a `google.rpc.ErrorInfo` detail.
 */
export function coralRefusal(code: Code, reason: string, metadata: Record<string, string> = {}) {
  const info = new BinaryWriter().tag(1, WireType.LengthDelimited).string(reason)
  for (const [key, value] of Object.entries(metadata)) {
    const entry = new BinaryWriter()
      .tag(1, WireType.LengthDelimited)
      .string(key)
      .tag(2, WireType.LengthDelimited)
      .string(value)
    info.tag(3, WireType.LengthDelimited).bytes(entry.finish())
  }
  const error = new ConnectError(reason, code)
  error.details.push({ type: 'google.rpc.ErrorInfo', value: info.finish() })
  return error
}
