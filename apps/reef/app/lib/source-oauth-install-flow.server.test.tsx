import { renderToString } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'

import { useOAuthInstallFlow } from './source-oauth-install-flow'

function OAuthFlowHarness() {
  useOAuthInstallFlow({
    fetchOAuthInstall: vi.fn(),
    onComplete: vi.fn(),
    openAuthorization: vi.fn(),
  })
  return null
}

describe('useOAuthInstallFlow server rendering', () => {
  it('does not read browser globals while rendering', () => {
    expect(() => renderToString(<OAuthFlowHarness />)).not.toThrow()
  })
})
