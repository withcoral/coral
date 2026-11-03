import { expect, it } from 'vitest'
import { authRouteTestArgs, localRuntimeConfig } from '@/auth/server-context.test-helper'
import { hostedRuntimeConfig } from '@/auth/test-config'
import { commitWorkspaceOnboarding } from './workspace-onboarding.server'

// Catches: a note outside the coral_web_ cookie namespace, one a sibling host can
// replace, or one that outlives its ten minutes.
it.each([
  ['a local deployment', localRuntimeConfig, null, 'coral_web_workspace_onboarding'],
  [
    'a hosted deployment',
    hostedRuntimeConfig,
    'coral-token',
    '__Host-coral_web_workspace_onboarding',
  ],
] as const)(
  'in %s, the onboarding note is %4$s for ten minutes',
  async (_, runtime, token, name) => {
    const { context } = authRouteTestArgs(new Request('http://reef.test/'), {}, token, runtime)

    const note = await commitWorkspaceOnboarding('workspaces/finance', context)

    expect(note.slice(0, note.indexOf('='))).toBe(name)
    expect(note).toMatch(/; Max-Age=600(;|$)/)
    expect(note).toMatch(/; HttpOnly(;|$)/)
  },
)
