// The addresses of the Tools page: every tool, one group, or one open tool.
// A provider's group and tools keep its resource path; the shared and the
// private group each have a fixed one. `routes.ts` declares these routes;
// what reads or builds such an address does it here.

import type { ProviderView, ToolView } from '@/views/providers/types'

/** The route segment that names the private group, after `tools/`. */
export const PRIVATE_GROUP = 'private'

/** The route segment that names the shared group, after `tools/`. */
export const SHARED_GROUP = 'shared'

/** Deleting a private tool returns to the private page with this flag in the query. */
export const PRIVATE_TOOL_DELETED = 'privateToolDeleted'

/** Publishing opens the shared tool with this flag in the query. */
export const SHARED_TOOL_PUBLISHED = 'sharedToolPublished'

/** Removing a shared tool returns to the shared page with this flag in the query. */
export const SHARED_TOOL_REMOVED = 'sharedToolRemoved'

/** The page of the private tools in a workspace. */
export function privateToolsPath(workspacePath: string) {
  return `${workspacePath}/tools/${PRIVATE_GROUP}`
}

/** The address that opens one of your own private tools. */
export function privateToolHref(workspacePath: string, path: string) {
  return `${privateToolsPath(workspacePath)}/${encodeURIComponent(path)}`
}

/** The page of the shared tools in a workspace. */
export function sharedToolsPath(workspacePath: string) {
  return `${workspacePath}/tools/${SHARED_GROUP}`
}

/** The address that opens one shared tool. */
export function sharedToolHref(workspacePath: string, path: string) {
  return `${sharedToolsPath(workspacePath)}/${encodeURIComponent(path)}`
}

/** Where the publish dialog reads its review and sends its publish. */
export function sharedToolPublishPath(workspacePath: string) {
  return `${workspacePath}/shared-tools/publish`
}

/**
 * Whether the group holds tools that a person or an agent wrote: the shared
 * or the private group. Its page has a fixed address, and its tools show
 * their source.
 */
export function holdsAuthoredTools(group: Pick<ProviderView, 'kind'>) {
  return group.kind === 'private' || group.kind === 'shared'
}

/**
 * The address of a tool's drawer. A shared or a private tool sits directly
 * under its group, and an agent's private tool names the agent after it.
 */
export function toolPath(group: Pick<ProviderView, 'kind' | 'path'>, tool: ToolView) {
  const name = encodeURIComponent(tool.relativePath)
  if (!holdsAuthoredTools(group)) return `${group.path}/tools/${name}`
  return tool.agent
    ? `${group.path}/${name}/agents/${encodeURIComponent(tool.agent.id)}`
    : `${group.path}/${name}`
}

/**
 * The tool of `group` that an address opens by its relative path. Two private
 * tools can share a path: without `agent`, the address opens your own.
 */
export function addressedTool(
  group: Pick<ProviderView, 'tools'>,
  relativePath: string,
  agent: string | null = null,
) {
  return group.tools.find(
    (tool) => tool.relativePath === relativePath && (tool.agent?.id ?? null) === agent,
  )
}

/**
 * The provider, and the tool if any, that a provider's own address names:
 * its tab, or one of its tools. Null for every other address.
 */
export function providerAddress(pathname: string) {
  const match = /\/providers\/([^/]+)(?:\/tools\/([^/?#]+))?\/?$/.exec(pathname)
  if (!match) return null
  return { provider: match[1]!, tool: match[2] ? decodeURIComponent(match[2]) : null }
}

// A provider's other pages, such as `new` and `settings`, are not the Tools page.
const PAGE =
  /^(\/workspaces\/[^/]+)\/(?:tools(?:\/[^/]+(?:\/[^/]+(?:\/agents\/[^/]+)?)?)?|providers\/(?!new(?:\/|$))[^/]+(?:\/tools\/[^/]+)?)$/
const GROUPS = [
  /^(\/workspaces\/[^/]+\/providers\/[^/]+)(?:\/tools\/[^/]+)?$/,
  /^(\/workspaces\/[^/]+\/tools\/[^/]+)(?:\/[^/]+(?:\/agents\/[^/]+)?)?$/,
]

/** The workspace whose Tools page this address is part of, by its path, or undefined. */
export function toolsPageWorkspace(pathname: string) {
  return PAGE.exec(pathname)?.[1]
}

/**
 * The group, by its own address, that this address names or opens a tool of.
 * Undefined for the tab of every group and for every other page.
 */
export function toolGroupAddress(pathname: string) {
  return GROUPS.map((group) => group.exec(pathname)?.[1]).find(Boolean)
}
