import { test } from 'vitest'
import { protectedDiscovery } from './oauth-mcp-discovery.ts'

// Fixture context requires test.for, not test.each.
test.for(['retain', 'different'] as const)(
  'Protected discovery and shared Add: %s authorization',
  protectedDiscovery,
)
