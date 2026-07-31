/**
 * Coverage for the security stage, end to end over the committed snapshot.
 *
 * The interesting question is not whether the YAML is well-formed but whether
 * it says the right thing: Slack lists a method's scopes without stating how
 * they relate, and the relationship differs per method.
 */

import { parse } from 'yaml'
import { describe, expect, it } from 'vitest'

import { loadConfig } from '../src/core/config.ts'
import { assertImportable, emitOpenApi, EmitError, OPENAPI_VERSION } from '../src/core/emit.ts'
import { extractApiModel } from '../src/adapters/slack/extract.ts'
import { applyOverlay, loadOverlay } from '../src/core/overlay.ts'
import { Snapshot } from '../src/core/snapshot.ts'

interface Document {
  components: {
    securitySchemes: Record<
      string,
      { type: string; flows: { authorizationCode: { scopes: Record<string, string> } } }
    >
  }
  paths: Record<string, Record<string, { security?: Record<string, string[]>[] }>>
}

const spec = await (async () => {
  const config = await loadConfig('slack')
  const snapshot = await Snapshot.open(config.snapshotDir)
  const model = applyOverlay(
    await extractApiModel(config, snapshot),
    await loadOverlay(config.overlayPath),
  )
  return parse(emitOpenApi(model, { version: '2026-07-30' })) as Document
})()

function securityOf(path: string): Record<string, string[]>[] {
  return spec.paths[path]?.get?.security ?? []
}

describe('securitySchemes', () => {
  it('declares one OAuth2 scheme per Slack token class', () => {
    expect(Object.keys(spec.components.securitySchemes).toSorted()).toEqual([
      'slackBotToken',
      'slackUserToken',
    ])
    for (const scheme of Object.values(spec.components.securitySchemes)) {
      expect(scheme.type).toBe('oauth2')
    }
  })

  it('carries the description Slack publishes for each scope', () => {
    const scopes = spec.components.securitySchemes.slackBotToken?.flows.authorizationCode.scopes

    expect(scopes?.['channels:read']).toBe(
      'View basic information about public channels in a workspace',
    )
  })

  /** Slack links to a page for identity:read that returns 404. */
  it('emits a scope with no published page rather than dropping it', () => {
    const scopes = spec.components.securitySchemes.slackUserToken?.flows.authorizationCode.scopes

    expect(scopes).toHaveProperty('identity:read')
    expect(scopes?.['identity:read']).toBe('')
  })
})

describe('operation security', () => {
  /**
   * conversations.list accepts any one of the four — each unlocks a
   * conversation type — so they are alternatives, not a required set.
   */
  it('expresses alternative scopes as separate requirements', () => {
    const security = securityOf('/conversations.list')

    expect(security).toContainEqual({ slackBotToken: ['channels:read'] })
    expect(security).toContainEqual({ slackBotToken: ['groups:read'] })
    for (const requirement of security) {
      expect(Object.values(requirement)[0]).toHaveLength(1)
    }
  })

  /**
   * team.externalTeams.list needs both, and nothing on the page says so — the
   * overlay does.
   */
  it('expresses jointly required scopes as one requirement', () => {
    expect(securityOf('/team.externalTeams.list')).toEqual([
      { slackBotToken: ['conversations.connect:manage', 'team:read'] },
    ])
  })

  it('offers both token classes where Slack documents both', () => {
    const schemes = new Set(securityOf('/conversations.list').flatMap((r) => Object.keys(r)))

    expect([...schemes].toSorted()).toEqual(['slackBotToken', 'slackUserToken'])
  })

  /** Slack omits the section entirely for a class a method does not accept. */
  it('offers only the user token where the method is user-only', () => {
    for (const path of ['/search.messages', '/reminders.list', '/team.accessLogs']) {
      const schemes = new Set(securityOf(path).flatMap((r) => Object.keys(r)))

      expect([...schemes], path).toEqual(['slackUserToken'])
    }
  })

  it('offers only the bot token where the method is bot-only', () => {
    const schemes = new Set(securityOf('/team.externalTeams.list').flatMap((r) => Object.keys(r)))

    expect([...schemes]).toEqual(['slackBotToken'])
  })

  it('gives every operation at least one requirement', () => {
    for (const [path, item] of Object.entries(spec.paths)) {
      expect(item.get?.security?.length ?? 0, `${path} declares no security`).toBeGreaterThan(0)
    }
  })
})

describe('assertImportable', () => {
  const base = {
    openapi: OPENAPI_VERSION,
    paths: {
      '/a': { get: { operationId: 'a/one', security: [{ scheme: ['read'] }] } },
    },
    components: {
      securitySchemes: {
        scheme: { type: 'oauth2', flows: { authorizationCode: { scopes: { read: 'Read.' } } } },
      },
    },
  }

  it('accepts security that resolves', () => {
    expect(() => assertImportable({ ...base })).not.toThrow()
  })

  it('rejects a reference to an undeclared scheme', () => {
    const document = {
      ...base,
      paths: { '/a': { get: { operationId: 'a/one', security: [{ other: ['read'] }] } } },
    }

    expect(() => assertImportable(document)).toThrow(/undeclared security scheme 'other'/)
  })

  /** The check that catches an operation gaining a scope upstream. */
  it('rejects a reference to a scope the scheme does not declare', () => {
    const document = {
      ...base,
      paths: { '/a': { get: { operationId: 'a/one', security: [{ scheme: ['write'] }] } } },
    }

    expect(() => assertImportable(document)).toThrow(
      /requires scope 'write', which 'scheme' does not declare/,
    )
  })

  it('reports the violation as an EmitError', () => {
    const document = {
      ...base,
      paths: { '/a': { get: { operationId: 'a/one', security: [{ nope: [] }] } } },
    }

    expect(() => assertImportable(document)).toThrow(EmitError)
  })
})
