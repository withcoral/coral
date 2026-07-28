import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'

import {
  FunctionRuntimeInvalidSchema,
  FunctionRuntimeReadySchema,
  FunctionSchema,
} from '@/generated/coral/v1/functions_pb'

import { toFunctionDetails } from './functions'

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
          sqlBody: 'select * from github.pull_requests',
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
      resultColumns: [
        { dataType: 'Int64', name: 'number', nullable: false },
        { dataType: 'Utf8', name: 'title', nullable: true },
      ],
      sources: [],
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
})
