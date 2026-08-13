import { create } from '@bufbuild/protobuf'
import { describe, expect, it, vi } from 'vitest'

import { authRouteTestArgs } from '@/auth/server-context.test-helper'

const { featureClientForRequest, listFeatures, setFeature } = vi.hoisted(() => ({
  featureClientForRequest: vi.fn(),
  listFeatures: vi.fn(),
  setFeature: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({
  featureClientForRequest,
}))

import { FeatureConfiguredState, FeatureStatusSchema } from '@/generated/coral/v1/features_pb'

import { action, loader, toRuntimeFeature } from './runtime-features'

describe('runtime features mapping', () => {
  it('shows the configured state, not the state the server is running', () => {
    const feature = create(FeatureStatusSchema, {
      // Toggled on since this server started, so `enabled` and `active` disagree.
      active: false,
      configured: FeatureConfiguredState.ENABLED,
      defaultEnabled: false,
      description: 'Enables installing and querying database sources.',
      enabled: true,
      key: 'database_sources',
    })

    expect(toRuntimeFeature(feature)).toEqual({
      description: 'Enables installing and querying database sources.',
      enabled: true,
      key: 'database_sources',
      label: 'Database sources',
    })
  })
})

describe('runtime features loader', () => {
  it('flags a restart when config has moved away from what the server booted with', async () => {
    listFeatures.mockResolvedValue({
      features: [featureStatus({ active: false, enabled: true, key: 'feedback' })],
    })
    featureClientForRequest.mockReturnValue({ listFeatures })

    const result = await loader(authRouteTestArgs(loadRequest(), {}))

    expect(result.restartPending).toBe(true)
  })

  it('does not ask for a restart while config and the running server agree', async () => {
    listFeatures.mockResolvedValue({
      features: [
        featureStatus({ active: false, enabled: false, key: 'feedback' }),
        // Enabled before this server started, so it is already running.
        featureStatus({ active: true, enabled: true, key: 'database_sources' }),
      ],
    })
    featureClientForRequest.mockReturnValue({ listFeatures })

    const result = await loader(authRouteTestArgs(loadRequest(), {}))

    expect(result.restartPending).toBe(false)
  })

  it('returns a load error as data rather than throwing', async () => {
    listFeatures.mockRejectedValue(new Error('coral is unreachable'))
    featureClientForRequest.mockReturnValue({ listFeatures })

    const result = await loader(authRouteTestArgs(loadRequest(), {}))

    expect(result).toEqual({
      features: [],
      loadError: 'coral is unreachable',
      restartPending: false,
    })
  })
})

describe('runtime features action', () => {
  it('persists the requested override', async () => {
    setFeature.mockResolvedValue({})
    featureClientForRequest.mockReturnValue({ setFeature })
    const request = toggleRequest('feedback', 'true')

    const result = await action(authRouteTestArgs(request, {}))

    expect(featureClientForRequest).toHaveBeenCalledWith(request, 'test-coral-token')
    expect(setFeature).toHaveBeenCalledWith(
      expect.objectContaining({ enabled: true, key: 'feedback' }),
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    )
    expect(result).toEqual({ key: 'feedback', status: 'success' })
  })

  it('treats any non-true value as a disable', async () => {
    setFeature.mockResolvedValue({})
    featureClientForRequest.mockReturnValue({ setFeature })

    await action(authRouteTestArgs(toggleRequest('feedback', 'false'), {}))

    expect(setFeature).toHaveBeenCalledWith(
      expect.objectContaining({ enabled: false, key: 'feedback' }),
      expect.anything(),
    )
  })

  it('returns a write error as data rather than throwing', async () => {
    setFeature.mockRejectedValue(new Error("unknown feature 'nope'"))
    featureClientForRequest.mockReturnValue({ setFeature })

    const result = await action(authRouteTestArgs(toggleRequest('nope', 'true'), {}))

    expect(result).toEqual({ key: 'nope', message: "unknown feature 'nope'", status: 'error' })
  })
})

function featureStatus(overrides: { active: boolean; enabled: boolean; key: string }) {
  return create(FeatureStatusSchema, {
    configured: FeatureConfiguredState.DEFAULT,
    description: 'A runtime feature.',
    ...overrides,
  })
}

function loadRequest() {
  return new Request('http://reef.test/settings/runtime-features')
}

function toggleRequest(key: string, enabled: string) {
  return new Request('http://reef.test/settings/runtime-features', {
    body: new URLSearchParams({ enabled, key }),
    method: 'POST',
  })
}
