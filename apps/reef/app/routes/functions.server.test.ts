import { create } from '@bufbuild/protobuf'
import { describe, expect, it, vi } from 'vitest'

import { authRouteTestArgs } from '@/auth/server-context.test-helper'

const { deleteFunction, functionClientForRequest } = vi.hoisted(() => ({
  deleteFunction: vi.fn(),
  functionClientForRequest: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({
  functionClientForRequest,
}))

import {
  FunctionRuntimeInvalidSchema,
  FunctionRuntimeReadySchema,
  FunctionSchema,
  FunctionTableFunctionPublishSchema,
} from '@/generated/coral/v1/functions_pb'

import { action, toFunctionDetails } from './functions'

describe('functions route mapping', () => {
  it('maps available runtime metadata to function details', () => {
    const fn = create(FunctionSchema, {
      name: 'review_queue',
      runtime: {
        case: 'ready',
        value: create(FunctionRuntimeReadySchema, {
          arguments: [
            { dataType: 'Utf8', name: 'owner' },
            { dataType: 'Utf8', name: 'repo' },
          ],
          description: 'Pull requests waiting for review.',
          resultColumns: [
            { dataType: 'Int64', name: 'number', nullable: false },
            { dataType: 'Utf8', name: 'title', nullable: true },
          ],
          sourceNames: ['github', 'slack'],
          sqlBody: 'select * from github.pull_requests',
          tableFunction: create(FunctionTableFunctionPublishSchema, {
            name: 'review_queue',
            schemaName: 'functions',
          }),
        }),
      },
    })

    expect(toFunctionDetails(fn)).toEqual({
      arguments: [
        { dataType: 'Utf8', name: 'owner' },
        { dataType: 'Utf8', name: 'repo' },
      ],
      body: 'select * from github.pull_requests',
      description: 'Pull requests waiting for review.',
      name: 'review_queue',
      namespace: 'functions',
      resultColumns: [
        { dataType: 'Int64', name: 'number', nullable: false },
        { dataType: 'Utf8', name: 'title', nullable: true },
      ],
      sources: ['github', 'slack'],
    })
  })

  it('omits functions that are not currently available', () => {
    const fn = create(FunctionSchema, {
      name: 'broken_function',
      runtime: {
        case: 'invalid',
        value: create(FunctionRuntimeInvalidSchema, { reason: 'Invalid SQL' }),
      },
    })

    expect(toFunctionDetails(fn)).toBeNull()
  })

  it('omits functions without a SQL namespace', () => {
    const fn = create(FunctionSchema, {
      name: 'missing_publish_target',
      runtime: {
        case: 'ready',
        value: create(FunctionRuntimeReadySchema),
      },
    })

    expect(toFunctionDetails(fn)).toBeNull()
  })
})

describe('functions route action', () => {
  it('deletes a function from the route workspace', async () => {
    deleteFunction.mockResolvedValue({})
    functionClientForRequest.mockReturnValue({ deleteFunction })
    const request = deleteRequest('review_queue')

    const result = await action(authRouteTestArgs(request, { workspaceId: 'analytics' }))

    expect(functionClientForRequest).toHaveBeenCalledWith(request, 'test-coral-token')
    expect(deleteFunction).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'review_queue',
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    )
    expect(result).toEqual({ name: 'review_queue', status: 'success' })
  })
})

function deleteRequest(name: string) {
  return new Request('http://reef.test/workspaces/analytics/functions', {
    body: new URLSearchParams({ name }),
    method: 'POST',
  })
}
