// The addresses of the Tools page: every tool, one group, or one open tool.
// A provider's group and tools keep its resource path; the private group has
// a fixed one. `routes.ts` declares these routes; what reads or builds such
// an address does it here.

import type { ProviderView, ToolView } from '@/views/providers/types'

/** The route segment that names the private group, after `tools/`. */
export const PRIVATE_GROUP = 'private'

/** Deleting a private tool returns to the private page with this flag in the query. */
export const PRIVATE_TOOL_DELETED = 'privateToolDeleted'

/** The page of the private tools in a workspace. */
export function privateToolsPath(workspacePath: string) {
  return `${workspacePath}/tools/${PRIVATE_GROUP}`
}

/** The address that opens one of your own private tools. */
export function privateToolHref(workspacePath: string, path: string) {
  return `${privateToolsPath(workspacePath)}/${encodeURIComponent(path)}`
}

/** The address of a tool's drawer. A private tool sits directly under its group. */
export function toolPath(group: Pick<ProviderView, 'kind' | 'path'>, tool: ToolView) {
  const name = encodeURIComponent(tool.relativePath)
  return group.kind === 'private' ? `${group.path}/${name}` : `${group.path}/tools/${name}`
}

/**
 * The tool of `group` that an address opens by its relative path. Two private
 * tools can share a path: the address opens your own, never an agent's.
 */
export function addressedTool(group: Pick<ProviderView, 'tools'>, relativePath: string) {
  return group.tools.find((tool) => tool.relativePath === relativePath && !tool.agent)
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
  /^(\/workspaces\/[^/]+)\/(?:tools(?:\/[^/]+(?:\/[^/]+)?)?|providers\/(?!new(?:\/|$))[^/]+(?:\/tools\/[^/]+)?)$/
const GROUPS = [
  /^(\/workspaces\/[^/]+\/providers\/[^/]+)(?:\/tools\/[^/]+)?$/,
  /^(\/workspaces\/[^/]+\/tools\/[^/]+)(?:\/[^/]+)?$/,
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
