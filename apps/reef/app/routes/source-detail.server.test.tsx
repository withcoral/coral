import { create } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { getSource, getSourceInfo, sourceClientForRequest } = vi.hoisted(() => {
  const getSourceMock = vi.fn()
  const getSourceInfoMock = vi.fn()
  return {
    getSource: getSourceMock,
    getSourceInfo: getSourceInfoMock,
    sourceClientForRequest: vi.fn(() => ({
      getSource: getSourceMock,
      getSourceInfo: getSourceInfoMock,
    })),
  }
})

vi.mock('@/lib/coral-request.server', () => ({ sourceClientForRequest }))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { SourceInfoSchema, SourceOrigin } from '@/generated/coral/v1/sources_pb'

import { loader } from './source-detail'

describe('source detail loader authentication', () => {
  beforeEach(() => {
    getSource.mockReset()
    getSourceInfo.mockReset()
    sourceClientForRequest.mockClear()
    getSource.mockResolvedValue({ source: undefined })
    getSourceInfo.mockResolvedValue({
      sourceInfo: create(SourceInfoSchema, {
        installed: false,
        name: 'github',
        origin: SourceOrigin.BUNDLED,
      }),
    })
  })

  it.each([
    ['hosted', 'coral-access-token'],
    ['local', null],
  ])('passes the %s request token to Coral', async (_mode, accessToken) => {
    const request = new Request('http://reef.test/workspaces/analytics/sources/github')

    const result = await loader(
      authRouteTestArgs(request, { sourceName: 'github', workspaceId: 'analytics' }, accessToken),
    )

    expect(sourceClientForRequest).toHaveBeenCalledWith(request, accessToken)
    expect(result.entry).toMatchObject({ installed: false, name: 'github' })
  })
})
