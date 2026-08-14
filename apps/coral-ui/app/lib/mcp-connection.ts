export type McpConnection =
  | { readonly mode: 'local' }
  | { readonly mode: 'remote'; readonly url: string }

const LOCAL_MODE = 'local'
const REMOTE_MODE = 'remote'

/**
 * Resolves the MCP server Coral UI installs into coding clients. This module is
 * importable by route modules with client exports, but this function runs only
 * from server loaders and resource routes.
 */
export function mcpConnectionFromEnv(env: NodeJS.ProcessEnv = process.env): McpConnection {
  const mode = env.CORAL_MCP_MODE === undefined ? LOCAL_MODE : env.CORAL_MCP_MODE.trim()

  if (mode === LOCAL_MODE) return { mode }
  if (mode !== REMOTE_MODE) {
    throw new Error('CORAL_MCP_MODE must be "local" or "remote".')
  }

  const configuredUrl = env.CORAL_MCP_URL?.trim()
  if (!configuredUrl) {
    throw new Error('CORAL_MCP_URL must be set when CORAL_MCP_MODE is "remote".')
  }

  let url: URL
  try {
    url = new URL(configuredUrl)
  } catch {
    throw new Error('CORAL_MCP_URL must be an absolute HTTPS URL.')
  }

  if (url.protocol !== 'https:' || url.username || url.password) {
    throw new Error('CORAL_MCP_URL must be an absolute HTTPS URL without credentials.')
  }

  return { mode, url: url.toString() }
}
