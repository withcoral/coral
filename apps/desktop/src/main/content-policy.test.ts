import { describe, expect, it } from 'vitest'

import {
  consentHandOff,
  formWindowStep,
  navigationDecision,
  permissionGranted,
  windowOpenDecision,
} from './content-policy'

const reef = 'http://127.0.0.1:41234'

describe('navigationDecision', () => {
  it.each([
    [`${reef}/workspaces`, reef, 'allow'],
    ['https://github.com/login/oauth/authorize', reef, 'open-externally'],
    ['http://127.0.0.1:41235/', reef, 'open-externally'],
    ['http://localhost:41234/', reef, 'open-externally'],
    [`${reef}/workspaces`, null, 'open-externally'],
    ['https://user:secret@example.com/', reef, 'deny'],
    // URL.origin leaves out credentials, so Reef's own origin needs the check too.
    ['http://user:secret@127.0.0.1:41234/workspaces', reef, 'deny'],
    ['file:///etc/passwd', reef, 'deny'],
    ['javascript:alert(1)', reef, 'deny'],
    ['data:text/html,hi', reef, 'deny'],
    ['vscode://extension/install', reef, 'deny'],
    ['not a url', reef, 'deny'],
  ] as const)('sends %s (Reef at %s) to %s', (target, origin, decision) => {
    expect(navigationDecision(target, origin)).toBe(decision)
  })
})

describe('windowOpenDecision', () => {
  it.each([
    // The system browser reaches the same loopback Reef.
    [`${reef}/workspaces`, false, reef, 'open-externally'],
    ['https://docs.withcoral.com/', false, reef, 'open-externally'],
    ['file:///etc/passwd', false, reef, 'deny'],
    // The system browser cannot repeat a POST, so a device sign-in form runs hidden.
    [`${reef}/workspaces/w/providers/new`, true, reef, 'submit-hidden'],
    ['https://upstream.example/device', true, reef, 'deny'],
    [`${reef}/workspaces/w/providers/new`, true, null, 'deny'],
    ['http://user:secret@127.0.0.1:41234/workspaces', true, reef, 'deny'],
  ] as const)('sends %s (form %s, Reef at %s) to %s', (target, form, origin, decision) => {
    expect(windowOpenDecision(target, form, origin, null)).toBe(decision)
  })

  const notes = 'https://github.com/withcoral/coral/releases/tag'
  it.each([
    // The sidebar notice names a newer release, which Desktop can install.
    [`${notes}/v1.4.0`, '1.3.0', { offerUpdate: '1.4.0' }],
    [`${notes}/v1.4.0`, null, 'open-externally'],
    // The notes of the version that runs have nothing to install.
    [`${notes}/v1.3.0`, '1.3.0', 'open-externally'],
    [`${notes}/v`, '1.3.0', 'open-externally'],
    [`${notes}/v1.4.0/assets`, '1.3.0', 'open-externally'],
    [`${notes}/v1%2F4`, '1.3.0', 'open-externally'],
    [`${notes}/1.4.0`, '1.3.0', 'open-externally'],
    [`${notes}/v1.4.0?x=1`, '1.3.0', 'open-externally'],
    [`${notes}/v1.4.0#notes`, '1.3.0', 'open-externally'],
    ['https://github.com/other/coral/releases/tag/v1.4.0', '1.3.0', 'open-externally'],
    ['http://github.com/withcoral/coral/releases/tag/v1.4.0', '1.3.0', 'open-externally'],
    ['https://user:secret@github.com/withcoral/coral/releases/tag/v1.4.0', '1.3.0', 'deny'],
  ] as const)('sends %s (Desktop %s) to %j', (target, running, decision) => {
    expect(windowOpenDecision(target, false, reef, running)).toEqual(decision)
  })

  it('sends a form for a release notes URL nowhere', () => {
    expect(windowOpenDecision(`${notes}/v1.4.0`, true, reef, '1.3.0')).toBe('deny')
  })
})

describe('formWindowStep', () => {
  it.each([
    ['hand-off', 302, 'close'],
    ['refuse', 302, 'close'],
    // A redirect may lead back to a Reef page, or off Reef where the guard stops it.
    ['pass', 303, 'wait'],
    ['pass', 307, 'wait'],
    ['pass', 200, 'show'],
    ['pass', 400, 'show'],
    ['pass', 304, 'show'],
  ] as const)('after %s with %i, the hidden window should %s', (handOff, status, step) => {
    expect(formWindowStep(handOff, status)).toBe(step)
  })
})

describe('permissionGranted', () => {
  it.each([
    ['clipboard-sanitized-write', `${reef}/workspaces`, true],
    ['clipboard-sanitized-write', 'https://example.com/', false],
    ['clipboard-read', `${reef}/workspaces`, false],
    ['media', `${reef}/workspaces`, false],
    ['notifications', `${reef}/workspaces`, false],
  ] as const)('answers %s from %s with %s', (permission, url, granted) => {
    expect(permissionGranted(permission, url, reef)).toBe(granted)
  })
})

describe('consentHandOff', () => {
  const consent = 'https://upstream.example/authorize?state=fixture'
  const progress = '/workspaces/w/accounts/authorizations/a'
  const consentRedirect = (headers: Record<string, string[]>, overrides = {}) => ({
    method: 'POST',
    resourceType: 'mainFrame',
    url: `${reef}/workspaces/w/accounts/connect`,
    statusCode: 302,
    headers: { 'set-cookie': ['callback=fixture; HttpOnly'], ...headers },
    ...overrides,
  })

  it('opens consent in the system browser and returns the window to the marked path', () => {
    const handOff = consentHandOff(
      consentRedirect({
        location: [consent],
        'x-coral-authorization-return-to': [progress],
      }),
      reef,
    )

    expect(handOff).toEqual({
      action: 'hand-off',
      consent,
      statusLine: 'HTTP/1.1 303 See Other',
      headers: { 'set-cookie': ['callback=fixture; HttpOnly'], location: [`${reef}${progress}`] },
    })
  })

  it('matches header names in any case', () => {
    const handOff = consentHandOff(
      consentRedirect({
        Location: [consent],
        'X-Coral-Authorization-Return-To': [progress],
      }),
      reef,
    )

    expect(handOff).toMatchObject({
      action: 'hand-off',
      consent,
      headers: { location: [`${reef}${progress}`] },
    })
    expect(Object.keys(handOff.action === 'hand-off' ? handOff.headers : {})).toEqual([
      'set-cookie',
      'location',
    ])
  })

  it.each([
    ['an unmarked redirect', consentRedirect({ location: [consent] })],
    [
      'a GET',
      consentRedirect(
        { location: [consent], 'x-coral-authorization-return-to': [progress] },
        { method: 'GET' },
      ),
    ],
    [
      'a frame',
      consentRedirect(
        { location: [consent], 'x-coral-authorization-return-to': [progress] },
        { resourceType: 'subFrame' },
      ),
    ],
    [
      'a response from another origin',
      consentRedirect(
        { location: [consent], 'x-coral-authorization-return-to': [progress] },
        { url: 'http://127.0.0.1:41235/connect' },
      ),
    ],
  ])('leaves %s alone', (_, response) => {
    expect(consentHandOff(response, reef)).toEqual({ action: 'pass' })
  })

  const marked = (location: string, returnTo: string) =>
    consentRedirect({ location: [location], 'x-coral-authorization-return-to': [returnTo] })

  it.each([
    ['a return path on another origin', marked(consent, '//other.example/progress')],
    ['an absolute return URL on another origin', marked(consent, 'https://other.example/progress')],
    [
      'a return path with credentials',
      marked(consent, 'http://user:secret@127.0.0.1:41234/progress'),
    ],
    [
      'credentials in the consent URL',
      marked('https://user:secret@upstream.example/authorize', progress),
    ],
    ['a consent URL with another scheme', marked('file:///etc/passwd', progress)],
    ['a javascript consent URL', marked('javascript:alert(1)', progress)],
    ['a malformed consent URL', marked('http://[', progress)],
    [
      'a marked redirect without a Location',
      consentRedirect({ 'x-coral-authorization-return-to': [progress] }),
    ],
    ['a marked page that is not a redirect', { ...marked(consent, progress), statusCode: 200 }],
    ['a marked 304, which is not a redirect', { ...marked(consent, progress), statusCode: 304 }],
  ])('refuses %s', (_, response) => {
    expect(consentHandOff(response, reef)).toEqual({ action: 'refuse' })
  })
})
