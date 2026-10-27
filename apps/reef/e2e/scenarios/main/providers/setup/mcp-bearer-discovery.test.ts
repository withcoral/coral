import { test } from 'vitest'
import { bearerDiscovery, discoveryCredentialScope } from './mcp-bearer-discovery.ts'

test('Add an MCP server that needs a bearer token before it lists its Tools', bearerDiscovery)

test.for(['caller', 'replaced'] as const)(
  'Workspace discovery credentials retain the exact %s authorization scope',
  discoveryCredentialScope,
)
