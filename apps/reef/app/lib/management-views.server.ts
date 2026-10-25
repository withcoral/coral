import {
  AgentAccessBlocker,
  AgentState,
  type Agent,
  type AgentRegistration,
} from '@/generated/coral/management/v1/agent_pb'
import {
  WorkspaceState,
  WorkspaceRole,
  type Workspace,
  type UserProfile,
} from '@/generated/coral/management/v1/workspace_pb'
import type {
  WorkspaceMembership,
  WorkspaceOwner,
} from '@/generated/coral/management/v1/membership_pb'
import {
  SavedFunctionDiagnosticCategory,
  type SavedFunction,
  type SavedFunctionDiagnostic,
  type SavedFunctionSummary,
} from '@/generated/coral/management/v1/saved_function_pb'
import type { Timestamp } from '@bufbuild/protobuf/wkt'
import type {
  SavedFunctionDiagnosticView,
  SavedFunctionSummaryView,
  SavedFunctionView,
} from '@/views/functions/types'
import { resourcePath } from './management.server'

export function workspaceView(w: Workspace) {
  return {
    name: w.name,
    displayName: w.displayName,
    state: enumLabel(w.state, {
      [WorkspaceState.ACTIVE]: 'active',
      [WorkspaceState.ARCHIVED]: 'archived',
    }),
    etag: w.etag,
    effectiveRole: roleLabel(w.effectiveRole),
    currentUser: w.currentUser,
    profile: profileView(w.currentUserProfile, w.currentUser),
    mcpEndpoint: w.mcpEndpoint,
    // Connect a client at `/mcp` where sign-in can pick the workspace; local
    // mode has no sign-in there, so it keeps the workspace URL.
    connectEndpoint: w.organizationMcpEndpoint || w.mcpEndpoint,
    path: resourcePath(w.name),
  }
}
/** The fields the sidebar's Workspace tree needs. */
export function sidebarWorkspaceView(w: Workspace) {
  return {
    displayName: w.displayName,
    path: resourcePath(w.name),
    effectiveRole: roleLabel(w.effectiveRole),
    mcpEndpoint: w.mcpEndpoint,
    etag: w.etag,
    state: workspaceView(w).state,
  }
}
/** An agent's settings page; its own path is its activity. */
export function agentSettingsPath(name: string) {
  return `${resourcePath(name)}/settings`
}
export function agentView(a: Agent) {
  return {
    name: a.name,
    displayName: a.displayName,
    state: enumLabel(a.state, {
      [AgentState.ENABLED]: 'enabled',
      [AgentState.DISABLED]: 'disabled',
    }),
    etag: a.etag,
    createdBy: a.createdBy,
    creator: profileView(a.creatorProfile, a.createdBy),
    accessBlockers: a.accessBlockers.map((blocker) =>
      enumLabel(blocker, {
        [AgentAccessBlocker.WORKSPACE_ARCHIVED]: 'Workspace archived',
        [AgentAccessBlocker.AGENT_DISABLED]: 'Agent disabled',
        [AgentAccessBlocker.API_SUSPENDED]: 'API access suspended',
        [AgentAccessBlocker.EXPIRED]: 'Access expired',
      }),
    ),
    path: resourcePath(a.name),
    settingsPath: agentSettingsPath(a.name),
  }
}
export function membershipView(m: WorkspaceMembership) {
  return {
    name: m.name,
    user: m.user,
    profile: profileView(m.profile, m.user),
    role: roleLabel(m.role),
    enabled: m.enabled,
    etag: m.etag,
  }
}
export function ownerView(owner: WorkspaceOwner) {
  return {
    user: owner.user,
    profile: profileView(owner.profile, owner.user),
    enabled: owner.enabled,
  }
}

export function profileView(profile: UserProfile | undefined, user: string) {
  return {
    user,
    name: profile?.displayName ?? '',
    email: profile?.email ?? '',
    local: profile?.local ?? false,
  }
}

function roleLabel(role: WorkspaceRole) {
  return enumLabel(role, { [WorkspaceRole.OWNER]: 'owner', [WorkspaceRole.MEMBER]: 'member' })
}
// Keep wire enums server-only. A newer or unspecified classification must not
// fall through to an enabled state or an actionable role in browser view models.
function enumLabel(value: number, labels: Partial<Record<number, string>>): string {
  const label = labels[value]
  if (label === undefined)
    throw new Response('Unrecognized management classification', { status: 503 })
  return label
}

export function registrationView(r: AgentRegistration) {
  return {
    name: r.name,
    uid: r.uid,
    enabled: r.enabled,
    etag: r.etag,
    expiration: isoTime(r.expireTime),
  }
}

/** The browser path of one of the current User's functions in a Workspace. */
export function savedFunctionHref(workspacePath: string, path: string) {
  return `${workspacePath}/functions/${encodeURIComponent(path)}`
}
export function savedFunctionSummaryView(
  workspacePath: string,
  f: SavedFunctionSummary,
): SavedFunctionSummaryView {
  return {
    path: f.path,
    description: f.description,
    savedAt: isoTime(f.updateTime),
    href: savedFunctionHref(workspacePath, f.path),
  }
}
export function savedFunctionView(f: SavedFunction): SavedFunctionView {
  return {
    path: f.path,
    description: f.description,
    declaration: f.declaration,
    source: f.source,
    diagnostics: f.diagnostics.map(diagnosticView),
    diagnosticsTruncated: f.diagnosticsTruncated,
    savedAt: isoTime(f.updateTime),
  }
}
function diagnosticView(d: SavedFunctionDiagnostic): SavedFunctionDiagnosticView {
  return {
    category: enumLabel(d.category, {
      [SavedFunctionDiagnosticCategory.ERROR]: 'error',
      [SavedFunctionDiagnosticCategory.WARNING]: 'warning',
      [SavedFunctionDiagnosticCategory.SUGGESTION]: 'suggestion',
      [SavedFunctionDiagnosticCategory.MESSAGE]: 'message',
    }) as SavedFunctionDiagnosticView['category'],
    code: Number(d.code),
    message: d.message,
    line: d.line === undefined ? null : Number(d.line),
    column: d.column === undefined ? null : Number(d.column),
  }
}
function isoTime(time: Timestamp | undefined) {
  return time ? new Date(Number(time.seconds) * 1000 + time.nanos / 1_000_000).toISOString() : ''
}
