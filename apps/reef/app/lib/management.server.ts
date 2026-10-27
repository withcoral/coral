import { createHash, randomUUID } from 'node:crypto'
import { createClient, Code, ConnectError } from '@connectrpc/connect'
import { data, type RouterContextProvider } from 'react-router'
import { validateCsrfToken } from '@/auth/csrf.server'
import { requestAuthContext } from '@/auth/server-context'
import {
  AgentService,
  AgentRegistrationService,
  AgentState,
} from '@/generated/coral/management/v1/agent_pb'
import { WorkspaceService, WorkspaceRole } from '@/generated/coral/management/v1/workspace_pb'
import { OrganizationMembershipService } from '@/generated/coral/management/v1/organization_membership_pb'
import { OrganizationWorkspaceService } from '@/generated/coral/management/v1/organization_workspace_pb'
import { WorkspaceMembershipService } from '@/generated/coral/management/v1/membership_pb'
import { ProviderService, ProviderSetupService } from '@/generated/coral/management/v1/provider_pb'
import { ProviderAccountService } from '@/generated/coral/management/v1/account_pb'
import { ActivityService } from '@/generated/coral/management/v1/activity_pb'
import { ToolPolicyService } from '@/generated/coral/management/v1/policy_pb'
import { SavedFunctionService } from '@/generated/coral/management/v1/saved_function_pb'
import { managementTransportForRequest } from './coral-request.server'
import { runtimeConfigContext } from './runtime-config.server'

export const privateHeaders = { 'Cache-Control': 'private, no-store', Pragma: 'no-cache' }
export function management(request: Request, context: Readonly<RouterContextProvider>) {
  const auth = context.get(requestAuthContext)
  const endpoint = context.get(runtimeConfigContext).endpoint
  const transport = managementTransportForRequest(request, auth, endpoint)
  // Provider requests include a bounded 30-second source fetch and synchronous catalog work.
  // Keep ordinary management requests short without cutting off supported Provider imports.
  const providerTransport = managementTransportForRequest(request, auth, endpoint, 60_000)
  return {
    workspaces: createClient(WorkspaceService, transport),
    activity: createClient(ActivityService, transport),
    organizationMemberships: createClient(OrganizationMembershipService, transport),
    organizationWorkspaces: createClient(OrganizationWorkspaceService, transport),
    memberships: createClient(WorkspaceMembershipService, transport),
    agents: createClient(AgentService, transport),
    registrations: createClient(AgentRegistrationService, transport),
    providers: createClient(ProviderService, providerTransport),
    setups: createClient(ProviderSetupService, providerTransport),
    accounts: createClient(ProviderAccountService, providerTransport),
    policies: createClient(ToolPolicyService, transport),
    savedFunctions: createClient(SavedFunctionService, transport),
  }
}
export type ManagementApi = ReturnType<typeof management>
export function formContext(context: Readonly<RouterContextProvider>) {
  const auth = context.get(requestAuthContext)
  return { csrf: auth.mode === 'required' ? auth.csrfToken : '', requestId: randomUUID() }
}
/**
 * A local deployment has one person: the implicit local User, who owns the
 * organization. Nobody else can sign in, so it has no people to manage.
 */
export function isLocalDeployment(context: Readonly<RouterContextProvider>) {
  return context.get(requestAuthContext).mode === 'disabled'
}
/** People pages do not exist in a local deployment. */
export function assertPeopleManagement(context: Readonly<RouterContextProvider>) {
  if (isLocalDeployment(context))
    throw new Response('Not found', { status: 404, headers: privateHeaders })
}
export function assertManagementOrigin(request: Request, context: Readonly<RouterContextProvider>) {
  const config = context.get(runtimeConfigContext).auth
  const origin = new URL(config.mode === 'required' ? config.publicUrl : request.url).origin
  if (request.headers.get('origin') !== origin)
    throw new Response('Invalid request origin', { status: 403, headers: privateHeaders })
}
export async function mutationForm(request: Request, context: Readonly<RouterContextProvider>) {
  if (request.method !== 'POST')
    throw new Response('Method Not Allowed', { status: 405, headers: privateHeaders })
  assertManagementOrigin(request, context)
  const config = context.get(runtimeConfigContext).auth
  const auth = context.get(requestAuthContext)
  if (
    config.mode === 'required' &&
    (auth.mode !== 'required' || !(await validateCsrfToken(request, config, auth.session)))
  ) {
    throw new Response('Invalid CSRF token', { status: 403, headers: privateHeaders })
  }
  return request.formData()
}
export function field(form: FormData, name: string) {
  const value = form.get(name)
  if (typeof value !== 'string' || value.length > 4096)
    throw new Response('Invalid form', { status: 400, headers: privateHeaders })
  return value
}
export function membershipRole(value: string): WorkspaceRole {
  switch (value) {
    case 'owner':
      return WorkspaceRole.OWNER
    case 'member':
      return WorkspaceRole.MEMBER
    default:
      throw new Response('Invalid Membership role', { status: 400, headers: privateHeaders })
  }
}
export function agentState(value: string): AgentState {
  switch (value) {
    case 'enabled':
      return AgentState.ENABLED
    case 'disabled':
      return AgentState.DISABLED
    default:
      throw new Response('Invalid Agent state', { status: 400, headers: privateHeaders })
  }
}
// A malformed identifier in the URL names nothing, so it reads as not found
// rather than as a bad form submission or an outage.
const WORKSPACE_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const AGENT_ID = /^[A-Za-z0-9][A-Za-z0-9:._-]{0,127}$/
export function workspaceName(id: string) {
  if (!WORKSPACE_ID.test(id))
    throw new Response('Not found', { status: 404, headers: privateHeaders })
  return `workspaces/${id}`
}
export function agentName(workspace: string, agent: string) {
  if (!AGENT_ID.test(agent))
    throw new Response('Not found', { status: 404, headers: privateHeaders })
  return `${workspaceName(workspace)}/agents/${agent}`
}
/** An agent's one Registration, which holds its API key, access and expiration. */
export function registrationName(agent: string) {
  return `${agent}/registration`
}
export function resourcePath(name: string) {
  return `/${name.split('/').map(encodeURIComponent).join('/')}`
}

const errors: Partial<Record<Code, [number, string]>> = {
  [Code.NotFound]: [404, 'This item does not exist.'],
  [Code.AlreadyExists]: [
    409,
    'This person is already a member. Use their actions menu to change their role.',
  ],
  [Code.PermissionDenied]: [403, 'You do not have permission for this action.'],
  [Code.Unauthenticated]: [401, 'Sign in again to continue.'],
  [Code.Aborted]: [
    409,
    'This item changed. Reload the page. Review the changes before you try again.',
  ],
  [Code.InvalidArgument]: [400, 'Check the form fields.'],
  [Code.FailedPrecondition]: [
    409,
    'The current settings do not allow this change. Reload the page. Check the settings before you try again.',
  ],
}
export function managementFailure(error: unknown): never {
  if (error instanceof Response) throw error
  // A defect in Reef itself must not hide behind an outage message.
  if (!(error instanceof ConnectError)) console.error('Unexpected failure in a Reef loader:', error)
  const [status, message] =
    error instanceof ConnectError
      ? (errors[error.code] ?? [503, 'Coral is not available.'])
      : [503, 'Coral is not available.']
  throw new Response(message, { status, headers: privateHeaders })
}
/**
 * Why Coral refused a change. A specific reason stands alone; a generic one
 * follows the tool that failed.
 */
export function failureReason(
  error: unknown,
  tool = 'Coral could not apply this change. ',
  notFoundMessage?: string,
): [status: number, message: string] {
  if (error instanceof Response) throw error
  const [status, message] =
    error instanceof ConnectError
      ? (errors[error.code] ?? [503, 'Coral is not available.'])
      : [503, 'Coral is not available.']
  const reasons: Record<string, string> = {
    AMBIGUOUS_EMAIL: 'More than one person matches this email. Coral granted no access.',
    WORKSPACE_ARCHIVED: 'This workspace is archived. Restore it before you make changes.',
    LAST_WORKSPACE_OWNER: 'This workspace must keep one owner. Assign another owner first.',
    OWNER_TRANSFER_REQUIRED:
      'This person is the only owner of a workspace. Choose a person to take over their workspaces.',
    OWNER_TARGET_NOT_ENABLED:
      'This person does not have an enabled organization membership. Choose another person.',
    REQUEST_ALREADY_APPLIED:
      'Coral already applied this request. Check the current list before you start another action.',
  }
  return [
    status,
    error instanceof ConnectError && reasons[error.rawMessage]
      ? reasons[error.rawMessage]
      : `${tool}${error instanceof ConnectError && error.code === Code.NotFound && notFoundMessage ? notFoundMessage : message}`,
  ]
}
export function actionFailure(
  error: unknown,
  tool = 'Coral could not apply this change. ',
  notFoundMessage?: string,
) {
  const [status, message] = failureReason(error, tool, notFoundMessage)
  return data({ error: message, credential: null }, { status, headers: privateHeaders })
}

/**
 * One request per person in a batch form, each with its own idempotency key.
 * Deriving it from the form's key keeps a resubmitted form from applying
 * anything twice.
 */
export function requestIdFor(requestId: string, key: string) {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(requestId))
    throw new Response('Invalid form', { status: 400, headers: privateHeaders })
  const hex = createHash('sha256').update(`${requestId}\0${key}`).digest('hex')
  // UUIDv8: custom bits, with the version and RFC 9562 variant set.
  const variant = ((parseInt(hex[16], 16) & 0x3) | 0x8).toString(16)
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-8${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`
}
