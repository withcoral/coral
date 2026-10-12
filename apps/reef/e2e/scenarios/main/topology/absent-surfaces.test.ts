import { test } from 'vitest'
import { absentSurfaces } from './absent-surfaces.ts'

test('The remote deployment exposes no extra MCP route or public gRPC method', absentSurfaces)
