import { test } from 'vitest'
import { protectedDiscovery } from '../../main/providers/setup/oauth-mcp-discovery.ts'

test.for(['retain', 'different'] as const)(
  'Local protected discovery and shared Add: %s authorization',
  protectedDiscovery,
)
