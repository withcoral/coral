import { describe, expect, it } from 'vitest'

import { URL_LIMIT, resolvedUrl, selectedUrl, serverChoices, serverProblem } from './server-choices'
import type { ServerGroup, ServerSelection, ServerVariable } from './server-choices'

function variable(name: string, defaultValue: string, values: string[] = []): ServerVariable {
  return { name, defaultValue, enumerated: values.length > 0, values }
}

function group(options: ServerGroup['options']): ServerGroup {
  return { declaration: '#/servers', tools: ['metrics'], options }
}

const site = variable('site', 'datadoghq.com', ['datadoghq.com', 'datadoghq.eu'])
const subdomain = variable('subdomain', 'api')

describe('serverChoices', () => {
  it('offers one URL for each value a listed variable declares', () => {
    const { urls, open } = serverChoices(
      group([{ url: 'https://{subdomain}.{site}', variables: [subdomain, site] }]),
      undefined,
    )

    expect(urls.map((choice) => choice.url)).toEqual([
      'https://api.datadoghq.com',
      'https://api.datadoghq.eu',
    ])
    expect(urls[1]).toMatchObject({
      template: 'https://{subdomain}.{site}',
      variables: { site: 'datadoghq.eu', subdomain: 'api' },
    })
    expect(open).toEqual([])
  })

  it('asks for a variable the document does not list, once its server is chosen', () => {
    const declared = group([{ url: 'https://{subdomain}.{site}', variables: [subdomain, site] }])
    const selection: ServerSelection = {
      declaration: '#/servers',
      url: 'https://{subdomain}.{site}',
      custom: false,
      variables: { site: 'datadoghq.eu', subdomain: 'intake' },
    }

    const { urls, open } = serverChoices(declared, selection)

    expect(open.map((item) => item.name)).toEqual(['subdomain'])
    // The person's own answer builds the URLs, not the document's default.
    expect(urls.map((choice) => choice.url)).toEqual([
      'https://intake.datadoghq.com',
      'https://intake.datadoghq.eu',
    ])
  })

  it('asks for every variable when the values make more URLs than the limit', () => {
    const many = variable(
      'site',
      'a0.example.test',
      Array.from({ length: URL_LIMIT + 1 }, (_, index) => `a${index}.example.test`),
    )
    const declared = group([{ url: 'https://{site}', variables: [many] }])

    const { urls, open } = serverChoices(declared, {
      declaration: '#/servers',
      url: 'https://{site}',
      custom: false,
      variables: { site: 'a3.example.test' },
    })

    expect(urls.map((choice) => choice.url)).toEqual(['https://a3.example.test'])
    expect(open.map((item) => item.name)).toEqual(['site'])
  })

  it('keeps one URL when two declared servers resolve to the same address', () => {
    const { urls } = serverChoices(
      group([
        { url: 'https://api.{site}', variables: [variable('site', 'example.test')] },
        { url: 'https://api.example.test', variables: [] },
      ]),
      undefined,
    )

    expect(urls.map((choice) => choice.url)).toEqual(['https://api.example.test'])
  })

  it('asks nothing for a URL the document does not declare', () => {
    const { open } = serverChoices(group([{ url: 'https://api.{site}', variables: [site] }]), {
      declaration: '#/servers',
      url: 'https://api.internal.test',
      custom: true,
      variables: { site: 'datadoghq.eu' },
    })

    expect(open).toEqual([])
  })
})

describe('resolvedUrl', () => {
  it('shows the variables that have no answer', () => {
    expect(resolvedUrl('https://{subdomain}.{site}', { site: 'example.test' })).toBe(
      'https://{subdomain}.example.test',
    )
  })

  it('sends requests to a typed URL as it is', () => {
    expect(
      selectedUrl({
        declaration: '#/servers',
        url: 'https://api.internal.test',
        custom: true,
        variables: { site: 'unused.test' },
      }),
    ).toBe('https://api.internal.test')
  })
})

describe('serverProblem', () => {
  const declared = group([
    { url: 'https://api.example.test/{path}', variables: [variable('path', '')] },
  ])
  const selection: ServerSelection = {
    declaration: '#/servers',
    url: declared.options[0].url,
    custom: false,
    variables: { path: '' },
  }

  it('accepts an empty declared value and expands it instead of leaving a placeholder', () => {
    expect(serverProblem(declared, selection)).toBeUndefined()
    expect(selectedUrl(selection)).toBe('https://api.example.test/')
    expect(serverChoices(declared, selection).urls[0].url).toBe('https://api.example.test/')
  })

  it('requires an answer when a declared variable is missing', () => {
    expect(serverProblem(declared, { ...selection, variables: {} })).toBe('Enter a value for path.')
  })

  it.each(['v1/records', 'two words', 'café'])('accepts the safe declared path %s', (path) => {
    expect(serverProblem(declared, { ...selection, variables: { path } })).toBeUndefined()
  })

  it('requires a root destination only when tools use it', () => {
    expect(serverProblem(declared, undefined)).toBeDefined()
    expect(serverProblem({ ...declared, tools: [] }, undefined)).toBeUndefined()
    expect(
      serverProblem({ ...declared, declaration: '#/paths/~1logs/servers' }, undefined),
    ).toBeUndefined()
  })

  it.each(['https://api.example.test/v1', 'http://localhost:8080'])(
    'accepts the custom base URL %s',
    (url) => {
      expect(serverProblem(declared, { ...selection, custom: true, url })).toBeUndefined()
    },
  )

  it.each([
    '',
    '/relative',
    'ftp://api.example.test',
    'https://api.example.test/{path}',
    'https://api.example.test?key=value',
    'https://api.example.test#fragment',
  ])('rejects the custom destination %s', (url) => {
    expect(serverProblem(declared, { ...selection, custom: true, url })).toBeDefined()
  })

  it('requires a declared server that still exists', () => {
    expect(
      serverProblem(declared, { ...selection, url: 'https://removed.example.test' }),
    ).toContain('no longer declares')
  })

  it('rejects variables removed from a retained server template', () => {
    expect(
      serverProblem(declared, { ...selection, variables: { path: '', removed: 'old' } }),
    ).toBeDefined()
  })

  it('rejects unsafe declared variables before confirmation', () => {
    for (const path of [
      'a?b',
      'a#b',
      'a@b',
      'a\\b',
      '{other}',
      'a\u0000b',
      'a\u0085b',
      'x'.repeat(2049),
      'é'.repeat(1025),
    ]) {
      expect(serverProblem(declared, { ...selection, variables: { path } })).toBeDefined()
    }
  })

  it('validates the expanded destination as well as each declared variable', () => {
    expect(
      serverProblem(declared, { ...selection, variables: { path: 'x'.repeat(2048) } }),
    ).toBeDefined()
    const host = group([{ url: 'https://{host}', variables: [variable('host', '')] }])
    expect(
      serverProblem(host, { ...selection, url: 'https://{host}', variables: { host: '' } }),
    ).toBeDefined()
  })

  it('rejects credentials and malformed custom destinations before confirmation', () => {
    for (const url of [
      'https://user:password@api.example.test',
      'https://user@api.example.test',
      'https://:password@api.example.test',
      'https://api.example.test:bad',
      'https://api.example.test/\\path',
      `https://api.example.test/${'x'.repeat(2048)}`,
    ]) {
      expect(serverProblem(declared, { ...selection, custom: true, url })).toBeDefined()
    }
  })

  it('accepts only values from a declared enum, including its empty value', () => {
    const enumerated = group([
      { ...declared.options[0], variables: [variable('path', '', ['', 'v1'])] },
    ])
    expect(serverProblem(enumerated, selection)).toBeUndefined()
    expect(serverProblem(enumerated, { ...selection, variables: { path: 'v2' } })).toContain(
      'Choose a value',
    )
  })
})
