import { describe, expect, it } from 'vitest'

import type { ConfigurationReviewView } from '@/views/providers/types'

import {
  acceptOperations,
  accountStep,
  attemptParams,
  attemptProgress,
  InvalidAttempt,
  parseAttempt,
  reconcileAttempt,
  reviseAttempt,
} from './model'

describe('recipe in an attempt URL', () => {
  it('keeps a recipe id through a round trip', () => {
    const attempt = parseAttempt('workspace', new URLSearchParams({ recipe: 'gitlab_self_hosted' }))
    expect(attempt.recipe).toBe('gitlab_self_hosted')
    expect(parseAttempt('workspace', attemptParams(attempt)).recipe).toBe('gitlab_self_hosted')
  })

  it('leaves the recipe out when the URL has none', () => {
    expect(parseAttempt('workspace', new URLSearchParams()).recipe).toBeUndefined()
  })

  // A recipe id reaches the library lookup and the next URL, so only recipe ids may pass.
  it.each(['GitHub', '1password', 'git-hub', '../github', `a${'b'.repeat(64)}`])(
    'rejects %s, which is not a recipe id',
    (recipe) => {
      expect(() => parseAttempt('workspace', new URLSearchParams({ recipe }))).toThrow(
        InvalidAttempt,
      )
    },
  )

  it('rejects a repeated recipe', () => {
    const params = new URLSearchParams([
      ['recipe', 'github'],
      ['recipe', 'stripe'],
    ])
    expect(() => parseAttempt('workspace', params)).toThrow(InvalidAttempt)
  })
})

/** A protected MCP server's Setup, awaiting a sign-in to read its operations. */
const mcpSetup: import('@/views/providers/types').SetupView = {
  id: '11111111-1111-4111-8111-111111111111',
  kind: 'mcp',
  state: 'awaiting_authorization',
  credentialMethod: 'oauth',
  credentialFields: [],
  authorized: false,
  grantedScope: '',
  authorization: null,
  discoveryFailure: '',
  expiresAt: '',
  sourceLabel: 'https://example.test/mcp',
  sourceHost: 'example.test',
  serverName: 'example.test',
}

describe('attemptProgress', () => {
  const setup = mcpSetup

  it('discovers protected operations before asking workspace account use', () => {
    const attempt = parseAttempt('workspace', new URLSearchParams({ setup: setup.id }))
    const ready = { ...setup, state: 'ready' as const, authorized: true }
    expect(attemptProgress(attempt, setup).stage).toBe('discovery')
    expect(
      attemptProgress(attempt, { ...setup, state: 'awaiting_discovery', authorized: true }).stage,
    ).toBe('discovery')
    expect(attemptProgress(attempt, ready).stage).toBe('operations')
    expect(attemptProgress(acceptOperations(attempt), ready).stage).toBe('account')
  })

  it.each(['fixed', 'caller'])('publishes only after the explicit %s account choice', (account) => {
    const attempt = parseAttempt(
      'workspace',
      new URLSearchParams({
        setup: setup.id,
        account,
        accountGrant: '33333333-3333-4333-8333-333333333333',
        operationsAccepted: '1',
      }),
    )
    expect(attemptProgress(attempt, { ...setup, state: 'ready', authorized: true }).stage).toBe(
      'publish',
    )
  })

  it('requires authorization for a shared account but lets callers connect later', () => {
    const attempt = parseAttempt(
      'workspace',
      new URLSearchParams({ setup: setup.id, account: 'fixed', operationsAccepted: '1' }),
    )
    expect(attemptProgress(attempt, { ...setup, state: 'ready' }).stage).toBe('account')
    expect(
      attemptProgress({ ...attempt, account: 'caller' }, { ...setup, state: 'ready' }).stage,
    ).toBe('publish')
  })

  it('does not promote a discovery grant by accepting operations', () => {
    const attempt = parseAttempt(
      'workspace',
      new URLSearchParams({ setup: setup.id, account: 'fixed' }),
    )
    const ready = { ...setup, state: 'ready' as const, authorized: true }
    expect(attemptProgress(attempt, ready).stage).toBe('operations')
    expect(attemptProgress(acceptOperations(attempt), ready).stage).toBe('account')
  })

  it('keeps personal grant review after accepting operations and returning from sign-in', () => {
    const attempt = parseAttempt(
      'workspace',
      new URLSearchParams({ setup: setup.id, account: 'caller', returnFromConnect: '1' }),
    )
    const accepted = acceptOperations(attempt)
    const returned = parseAttempt('workspace', attemptParams(accepted))
    expect(attemptProgress(returned, { ...setup, state: 'ready', authorized: true }).stage).toBe(
      'account',
    )
  })

  // Catches: the agent's setup link skipping the operations, landing on
  // configuration, or losing the namespace and route it asked for.
  it.each([
    ['an OAuth MCP source, shared', 'fixed', {}, 'discovery', 'discovery'],
    ['an OAuth MCP source, separate', 'caller', {}, 'discovery', 'discovery'],
    [
      'an OAuth MCP source after sign-in',
      'caller',
      { state: 'ready' as const, authorized: true },
      'operations',
      'account',
    ],
    [
      'a shared credential',
      'fixed',
      { kind: 'openapi' as const, state: 'ready' as const, credentialMethod: 'bearer' as const },
      'operations',
      'account',
    ],
  ])("opens the agent's link for %s at the operations", (_, route, over, stage, next) => {
    // The link Coral returns when an addProvider call needs sign-in names
    // only the Setup, which holds the namespace and route the Program asked for.
    const requested = {
      ...setup,
      ...over,
      request: { namespace: 'weather', route: route as 'fixed' | 'caller' },
    }
    const attempt = reconcileAttempt(
      parseAttempt('workspace', new URLSearchParams({ setup: setup.id })),
      requested,
    )
    expect(attempt).toMatchObject({ namespace: 'weather', account: null })
    expect(attemptProgress(attempt, requested).stage).toBe(stage)
    expect(attemptProgress(acceptOperations(attempt), requested).stage).toBe(next)
  })

  it("keeps a namespace the person chose over the Program's", () => {
    const requested = { ...setup, request: { namespace: 'weather', route: 'fixed' as const } }
    const attempt = parseAttempt(
      'workspace',
      new URLSearchParams({ setup: setup.id, namespace: 'forecast' }),
    )
    expect(reconcileAttempt(attempt, requested)).toMatchObject({ namespace: 'forecast' })
  })

  // Catches: the agent's link for a configuration confirmed without an OAuth
  // application, where Coral cannot set one up, passing the one step that asks
  // for a client ID and reaching a sign-in that cannot start.
  it.each([
    ['an MCP sign-in Coral cannot set up', {}, {}, 'configuration'],
    [
      'an API sign-in Coral cannot set up',
      { kind: 'openapi' as const, state: 'ready' as const },
      {},
      'configuration',
    ],
    [
      'a device sign-in, which Coral never sets up',
      { kind: 'openapi' as const, state: 'ready' as const, credentialMethod: 'device' as const },
      { automaticClientAvailable: true },
      'configuration',
    ],
    ['a sign-in Coral sets up', {}, { automaticClientAvailable: true }, 'discovery'],
    [
      'a sign-in already started with its application',
      {
        authorization: {
          state: 'failed' as const,
          requestedScopes: [],
          bootstrap: 'document' as const,
          failure: 'access_denied',
          clientId: 'https://coral.test/.well-known/oauth-client',
          expiresAt: '',
        },
      },
      {},
      'discovery',
    ],
    [
      'a sign-in configuration chose the application for',
      { kind: 'openapi' as const, state: 'ready' as const },
      {
        application: {
          mode: 'manual' as const,
          clientId: 'weather-app',
          authentication: 'none' as const,
          pkce: 's256' as const,
          parameters: '',
        },
      },
      'operations',
    ],
  ])("opens the agent's link for %s", (_, over, review, stage) => {
    const attempt = parseAttempt('workspace', new URLSearchParams({ setup: setup.id }))
    const configuration = {
      revision: '1',
      confirmed: true,
      automaticClientAvailable: false,
      ...review,
    } as ConfigurationReviewView
    const request = { namespace: 'weather', route: 'fixed' as const }
    expect(attemptProgress(attempt, { ...setup, ...over, configuration, request }).stage).toBe(
      stage,
    )
  })

  it('discovers an update without asking a creation-only account question', () => {
    const attempt = parseAttempt('update', new URLSearchParams({ setup: setup.id }), 'etag')
    expect(attemptProgress(attempt, setup).stage).toBe('discovery')
    expect(attemptProgress(attempt, { ...setup, state: 'ready', authorized: true }).stage).toBe(
      'publish',
    )
  })

  it('replays a consumed publication with the frozen account choice', () => {
    const attempt = parseAttempt(
      'workspace',
      new URLSearchParams({
        setup: setup.id,
        account: 'fixed',
        accountGrant: '33333333-3333-4333-8333-333333333333',
        requestId: '22222222-2222-4222-8222-222222222222',
        configurationRevision: '1',
      }),
    )
    expect(attemptProgress(attempt, { ...setup, state: 'consumed' })).toMatchObject({
      stage: 'publish',
      account: 'fixed',
      replaying: true,
      canRevise: false,
      ended: null,
    })
  })

  it.each(['expired', 'canceled', 'consumed'] as const)(
    'ends a draft attempt whose Setup is %s',
    (state) => {
      const attempt = parseAttempt('workspace', new URLSearchParams({ setup: setup.id }))
      expect(attemptProgress(attempt, { ...setup, state })).toMatchObject({
        stage: 'terminal',
        ended: state,
      })
    },
  )
})

describe('accepting the listed operations', () => {
  const setupId = '11111111-1111-4111-8111-111111111111'
  const ready: import('@/views/providers/types').SetupView = {
    id: setupId,
    kind: 'openapi',
    state: 'ready',
    credentialMethod: 'bearer',
    credentialFields: [],
    authorized: true,
    grantedScope: '',
    authorization: null,
    discoveryFailure: '',
    expiresAt: '',
    sourceLabel: 'https://api.example.test/openapi.json',
    sourceHost: 'api.example.test',
    serverName: 'api.example.test',
  }
  const draft = () => parseAttempt('workspace', new URLSearchParams({ setup: setupId }))

  it('keeps the acceptance through a URL round trip', () => {
    const accepted = acceptOperations(draft())
    expect(accepted.operationsAccepted).toBe(true)
    expect(attemptParams(accepted).get('operationsAccepted')).toBe('1')
    const returned = parseAttempt('workspace', attemptParams(accepted))
    expect(returned.state === 'draft' && returned.operationsAccepted).toBe(true)
    expect(attemptProgress(returned, ready).stage).toBe('account')
  })

  it('leaves the parameter out of an attempt that has not accepted the list', () => {
    expect(attemptParams(draft()).has('operationsAccepted')).toBe(false)
  })

  it('rejects any value other than the accepted marker', () => {
    expect(() =>
      parseAttempt('workspace', new URLSearchParams({ setup: setupId, operationsAccepted: 'yes' })),
    ).toThrow(InvalidAttempt)
  })

  it('relists the operations when configuration reopens, and keeps them otherwise', () => {
    const accepted = acceptOperations(draft())
    expect(reviseAttempt(accepted, 'configuration').operationsAccepted).toBe(false)
    expect(reviseAttempt(accepted, 'account').operationsAccepted).toBe(true)
    expect(reviseAttempt(accepted).operationsAccepted).toBe(true)
  })

  // A frozen attempt reached Review, which only an accepted draft can reach.
  // Reconciling a stale revision used to send it back to the operations step.
  it('keeps the acceptance when a frozen attempt is revised', () => {
    const frozen = parseAttempt(
      'workspace',
      new URLSearchParams({
        setup: setupId,
        account: 'caller',
        requestId: '22222222-2222-4222-8222-222222222222',
        configurationRevision: '1',
      }),
    )
    const revised = reviseAttempt(frozen)
    expect(revised.operationsAccepted).toBe(true)
    expect(attemptProgress(revised, ready).stage).toBe('publish')
  })

  it('never asks an update to accept a list it cannot act on', () => {
    const attempt = parseAttempt('update', new URLSearchParams({ setup: setupId }), 'etag')
    expect(attempt.state === 'draft' && attempt.operationsAccepted).toBe(false)
    expect(attemptProgress(attempt, ready).stage).toBe('publish')
  })
})

/** A Setup a Program's addProvider call prepared, asking for `route`. */
const requested = (route: 'fixed' | 'caller') => ({ request: { namespace: 'weather', route } })

describe('accountStep', () => {
  const grant = '33333333-3333-4333-8333-333333333333'
  const setup = { ...mcpSetup, state: 'ready' as const }
  const held = { authorized: true, provisionalGrantId: grant }
  // Catches: a Program's Setup added with no "Add provider" click once a
  // sign-in lands, a person's own sign-in no longer adding it, or the account
  // question opening on an outcome nobody chose.
  it.each([
    ['nothing chosen yet', '', {}, 'shared-connect', false],
    ['nothing chosen, a sign-in in hand', '', held, 'own-keep', false],
    ['a shared account', 'account=fixed', {}, 'shared-connect', false],
    ['a shared account in hand', 'account=fixed', held, 'shared-keep', false],
    ['own accounts, later', 'account=caller', {}, 'own-later', false],
    ['own accounts, after Connect', 'account=caller&returnFromConnect=1', {}, 'own-connect', true],
    [
      'own accounts, back from Connect',
      'account=caller&returnFromConnect=1',
      held,
      'own-keep',
      true,
    ],
    ['shared, back from Connect', 'account=fixed&returnFromConnect=1', held, 'shared-keep', true],
    ["the Program's shared account", '', requested('fixed'), 'shared-connect', false],
    [
      "the Program's shared account in hand",
      '',
      { ...requested('fixed'), ...held },
      'shared-keep',
      false,
    ],
    ["the Program's own accounts", '', requested('caller'), 'own-connect', false],
    [
      "the Program's own accounts, back from Connect",
      'account=caller&returnFromConnect=1',
      { ...requested('caller'), ...held },
      'own-keep',
      false,
    ],
    [
      "a Program's route this Reef does not know, back from Connect",
      'account=caller&returnFromConnect=1',
      { request: { namespace: 'weather', route: null }, ...held },
      'own-keep',
      false,
    ],
    [
      "the Program's shared account, back from Connect",
      'account=fixed&returnFromConnect=1',
      { ...requested('fixed'), ...held },
      'shared-keep',
      false,
    ],
  ] as const)('for %s', (_, query, over, plan, publishOnReturn) => {
    const attempt = parseAttempt('workspace', new URLSearchParams(`setup=${setup.id}&${query}`))
    expect(accountStep(attempt, { ...setup, ...over })).toEqual({
      reopens: query.includes('returnFromConnect'),
      plan,
      publishOnReturn,
    })
  })
})
