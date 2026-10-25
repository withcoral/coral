export interface PersonView {
  user: string
  name: string
  email: string
  local: boolean
}
/** A parent page in a breadcrumb trail. */
export interface Crumb {
  label: string
  to: string
}
export interface FormContext {
  csrf: string
  requestId: string
}
export interface ActionResult {
  error: string | null
  credential: string | null
  message?: string
  /** The people a batch refused, so the dialog keeps only them for a retry. */
  refused?: string[]
  agentDisplayName?: string
  mcpEndpoint?: string
}
export interface WorkspaceView {
  name: string
  displayName: string
  state: string
  etag: string
  effectiveRole: string
  currentUser: string
  profile: PersonView
  /** This workspace's own MCP URL. */
  mcpEndpoint: string
  /** The MCP URL a client connects at: `/mcp`, or the workspace URL in local mode. */
  connectEndpoint: string
  path: string
}
export interface MembershipView {
  name: string
  user: string
  profile: PersonView
  role: string
  /** False when the person's organization membership is disabled. */
  enabled: boolean
  etag: string
}
/** A designated owner of a workspace. */
export interface OwnerView {
  user: string
  profile: PersonView
  /** False when the person's organization membership is disabled. */
  enabled: boolean
}
/** An enabled organization member, by organization membership name. */
export interface OrganizationPersonView {
  name: string
  profile: PersonView
}
/** A workspace as an organization owner sees it. */
export interface OrganizationWorkspaceView {
  name: string
  displayName: string
  state: 'active' | 'archived'
  etag: string
  owners: OwnerView[]
}
export interface AgentView {
  name: string
  displayName: string
  state: string
  etag: string
  createdBy: string
  creator: PersonView
  accessBlockers: string[]
  path: string
  settingsPath: string
}
export interface RegistrationView {
  name: string
  uid: string
  enabled: boolean
  etag: string
  expiration: string
}
