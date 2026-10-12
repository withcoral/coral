import { test } from 'vitest'
import {
  bearerDiscovery,
  discoveryCredentialScope,
} from '../../main/providers/setup/mcp-bearer-discovery.ts'

test('Local Add of an MCP server that needs a bearer token to list its Operations', bearerDiscovery)

test.for(['caller', 'replaced'] as const)(
  'Local Workspace discovery credentials retain the exact %s authorization scope',
  discoveryCredentialScope,
)
