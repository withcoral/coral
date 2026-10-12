import { test } from 'vitest'
import { absentSurfaces } from '../../main/topology/absent-surfaces.ts'

test('The local deployment exposes no extra MCP route or public gRPC method', absentSurfaces)
