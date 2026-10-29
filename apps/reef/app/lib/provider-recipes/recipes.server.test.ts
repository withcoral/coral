import * as fs from 'node:fs'
import * as path from 'node:path'

import { afterEach, describe, expect, it, vi } from 'vitest'

import type { SetupView } from '@/views/providers/types'

import { byService } from './choice'
import { CLIENT_CONFIG_HINT_GENERATORS } from './client-config-hints'
import {
  recipeConfiguration as configurationFor,
  recipeDocumentationUrl,
} from './configuration.server'
import { defaultAnswers, filledRecipe, refilled, withRecipe } from './fill-inputs'
import { OPENAPI_DOCUMENT_MAX_BYTES } from '../provider-input-limits'
import {
  PROVIDER_RECIPES,
  RECIPE_CHOICES,
  recipeById,
  recipeChoice,
  recipeDocument,
} from './library.server'
import { parseRecipe } from './parse-recipe'
import { recipeJsonSchema, type Recipe } from './recipe-format'

const reefRoot = path.resolve(import.meta.dirname, '../../..')
const recipeLibraries = ['recipes']

function recipeFiles() {
  return recipeLibraries.flatMap((library) => {
    const libraryPath = path.join(reefRoot, library)
    return fs
      .readdirSync(libraryPath, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => path.join(libraryPath, entry.name, 'recipe.yaml'))
  })
}

/** A YAML flow map with count entries, for testing entry limits. */
function flowMap(count: number, entry: (index: number) => string) {
  return `{${Array.from({ length: count }, (_, index) => entry(index)).join(', ')}}`
}

function problemsFor(text: string) {
  const result = parseRecipe(text)
  return result.ok ? [] : result.problems
}

describe('checked-in recipes', () => {
  const files = recipeFiles()

  it('finds the recipe library', () => {
    expect(files.length).toBeGreaterThan(0)
  })

  it.each(files.map((file) => [path.relative(reefRoot, file), file]))(
    '%s is a valid recipe',
    (_name, file) => {
      const result = parseRecipe(fs.readFileSync(file, 'utf8'))
      expect(result.ok ? [] : result.problems).toEqual([])
      if (!result.ok) return

      const { recipe } = result
      const directory = path.dirname(file)
      expect(recipe.id).toBe(path.basename(directory))

      for (const icon of [recipe.icons?.light, recipe.icons?.dark]) {
        if (icon !== undefined) expect(fs.existsSync(path.join(directory, icon)), icon).toBe(true)
      }

      // Coral refuses a larger document, so the recipe could never be read.
      if ('openapi' in recipe.source && 'file' in recipe.source.openapi) {
        const document = path.join(directory, recipe.source.openapi.file)
        expect(fs.statSync(document).size).toBeLessThanOrEqual(OPENAPI_DOCUMENT_MAX_BYTES)
      }
    },
  )
})

describe('bundled recipe library', () => {
  it('offers tenant recipes with only the questions needed to read their source', () => {
    const parsed = parseRecipe(`schema_version: 1
id: tenant_source
display_name: Tenant source
source:
  mcp:
    url: https://{{input.tenant}}.example.test/{{input.region}}/mcp
inputs:
  tenant:
    type: string
    label: Tenant
  region:
    type: string
    label: Region
    default: us
    enum: [us, eu]
  later:
    type: string
    label: Account
`)
    if (!parsed.ok) throw new Error(parsed.problems.join('; '))
    expect(recipeChoice(parsed.recipe)).toMatchObject({
      id: 'tenant_source',
      sourceInputs: [
        { name: 'tenant', label: 'Tenant', defaultValue: '', choices: [] },
        { name: 'region', label: 'Region', defaultValue: 'us', choices: ['us', 'eu'] },
      ],
    })
  })
  it('keeps API and MCP labels on source buttons rather than recipe display names', () => {
    for (const recipe of PROVIDER_RECIPES) {
      expect(recipe.display_name, recipe.id).not.toMatch(/\b(?:API|MCP)\b/)
    }
  })

  it('loads every recipe in recipes/', () => {
    const directories = fs
      .readdirSync(path.join(reefRoot, 'recipes'), { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)
      .toSorted()
    expect(PROVIDER_RECIPES.map((recipe) => recipe.id).toSorted()).toEqual(directories)
  })

  // A card takes its name and icon from the recipe named after the service,
  // tells its buttons apart only as MCP and API, and names the provider alike from either.
  it('gives every shared service a recipe of its own name, one recipe for each kind and one display name', () => {
    for (const choice of RECIPE_CHOICES) {
      if (choice.service) expect(recipeById(choice.service)?.id, choice.id).toBe(choice.service)
    }
    const shared = byService(RECIPE_CHOICES).filter(({ choices }) => choices.length > 1)
    expect(shared.length).toBeGreaterThan(0)
    for (const { service, shown, choices } of shared) {
      expect(shown.id, service).toBe(service)
      expect(choices.map((choice) => choice.kind).toSorted(), service).toEqual(['mcp', 'openapi'])
      expect(new Set(choices.map((choice) => choice.displayName)), service).toEqual(
        new Set([shown.displayName]),
      )
    }
  })

  it.each([
    [' team ', 'tenant=team'],
    ['   ', ''],
  ])('normalizes MCP source header input default %j before discovery', (answer, expected) => {
    const parsed = parseRecipe(`schema_version: 1
id: header_defaults
display_name: Header defaults
source:
  mcp:
    url: https://example.test/mcp
inputs:
  tenant:
    type: string
    label: Tenant
    default: '${answer}'
request_headers:
  x-tenant: 'tenant={{input.tenant}}'
`)
    if (!parsed.ok) throw new Error(parsed.problems.join('; '))
    expect(recipeChoice(parsed.recipe)?.requestHeaders).toEqual([
      { name: 'x-tenant', value: expected },
    ])
  })

  // Without a bundled URL the catalog shows a lettermark in place of the recipe's icon.
  it('gives every catalog recipe with an icon a URL for it', () => {
    const withIcons = RECIPE_CHOICES.filter((choice) => recipeById(choice.id)?.icons !== undefined)
    expect(withIcons.length).toBeGreaterThan(0)
    for (const choice of withIcons) {
      expect(choice.icons?.light, choice.id).toBeTruthy()
    }
    const withDarkIcons = RECIPE_CHOICES.filter(
      (choice) => recipeById(choice.id)?.icons?.dark !== undefined,
    )
    expect(withDarkIcons.length).toBeGreaterThan(0)
    for (const choice of withDarkIcons) {
      expect(choice.icons?.dark, choice.id).toBeTruthy()
    }
  })

  // Reef sends a bundled document's text to Coral; without it the recipe cannot be read.
  it('bundles the document of every catalog recipe that names one', () => {
    const bundled = RECIPE_CHOICES.filter((choice) => 'document' in choice)
    expect(bundled.length).toBeGreaterThan(0)
    for (const choice of bundled) {
      expect(recipeDocument(choice.id), choice.id).toBeTruthy()
    }
  })

  it('has no bundled document for a recipe that reads a URL', () => {
    expect(recipeDocument('stripe')).toBeNull()
  })

  // An attempt URL can name a recipe the library no longer has; nothing then fills in.
  it('logs a recipe id the library does not have', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    expect(recipeById('retired_recipe')).toBeNull()
    expect(warn).toHaveBeenCalledOnce()
    expect(warn.mock.calls[0][0]).toMatch(/retired_recipe/)
    warn.mockRestore()
  })
})

describe('recipe JSON Schema', () => {
  it('matches the checked-in recipes/schema.json', () => {
    const checkedIn: unknown = JSON.parse(
      fs.readFileSync(path.join(reefRoot, 'recipes/schema.json'), 'utf8'),
    )
    expect(checkedIn, 'Run npm run recipes:schema --prefix apps/reef').toEqual(recipeJsonSchema())
  })
})

describe('parseRecipe', () => {
  const minimal = [
    'schema_version: 1',
    'id: example',
    'display_name: Example',
    'source:',
    '  openapi:',
    '    url: https://api.example.com/openapi.json',
    '',
  ].join('\n')

  it('accepts a minimal recipe', () => {
    expect(problemsFor(minimal)).toEqual([])
  })

  it('accepts an OpenAPI document bundled beside the recipe', () => {
    const text = minimal.replace('url: https://api.example.com/openapi.json', 'file: openapi.json')
    expect(problemsFor(text)).toEqual([])
  })

  // The file name selects bundled text by path, so it must not leave the recipe's directory.
  it.each([
    ['a path', 'file: ../stripe/openapi.json', /^source\.openapi\.file: /],
    ['the recipe itself', 'file: recipe.yaml', /^source\.openapi\.file: /],
    ['a file that is not JSON or YAML', 'file: openapi.txt', /^source\.openapi\.file: /],
    [
      'both a URL and a file',
      'url: https://api.example.com/openapi.json\n    file: openapi.json',
      /^source: /,
    ],
  ])('rejects a bundled document named by %s', (_, source, problem) => {
    const text = minimal.replace('url: https://api.example.com/openapi.json', source)
    expect(problemsFor(text)).toContainEqual(expect.stringMatching(problem))
  })

  it('rejects an unknown field', () => {
    expect(problemsFor(`${minimal}unknown: true\n`).join('\n')).toMatch(/unknown/)
  })

  it('rejects a duplicate YAML key', () => {
    expect(problemsFor(`${minimal}id: again\n`).join('\n')).toMatch(/unique/i)
  })

  it('rejects a reference to an undeclared input', () => {
    const text = minimal.replace(
      'https://api.example.com/openapi.json',
      '"https://{{input.site}}/openapi.json"',
    )
    expect(problemsFor(text)).toEqual(['input "site" is referenced but not declared'])
  })

  it('rejects a credential field that is never used', () => {
    const text = [
      minimal,
      'authentication:',
      '  options:',
      '    keys:',
      '      method: header_credentials',
      '      label: Keys',
      '      fields:',
      '        api_key: {label: API key}',
      '        spare: {label: Spare}',
      '      headers:',
      '        x-api-key: {field: api_key}',
      '',
    ].join('\n')
    expect(problemsFor(text)).toEqual([
      'authentication.options.keys: field "spare" is declared but never used',
    ])
  })

  function withOption(...lines: string[]) {
    return [`${minimal}authentication:`, '  options:', '    key:', ...lines, ''].join('\n')
  }

  const signIn = [
    '      method: oauth_authorization_code',
    '      label: Sign in',
    '      authorization_url: https://login.example.com/authorize',
    '      token_url: https://login.example.com/token',
  ]

  const siteInput = 'inputs:\n  site: {type: string, label: Site}\n'

  /** A one-field header_credentials option with one headers entry, such as `x-api-key: {field: key}`. */
  function withHeaderKey(entry: string) {
    return withOption(
      '      method: header_credentials',
      '      label: Key',
      '      fields: {key: {label: Key}}',
      `      headers: {${entry}}`,
    )
  }

  it('accepts a single key in a header after a prefix', () => {
    expect(
      problemsFor(withHeaderKey('authorization: {field: key, prefix: "Token token="}')),
    ).toEqual([])
  })

  // A single key is a one-field header_credentials or query_credentials option, and
  // no method puts a credential in the path (ADR 0002).
  it.each([
    ['header_api_key', '      header: x-api-key'],
    ['query_api_key', '      parameter: api_key'],
    ['path_token', '      path_prefix: /bot'],
  ])('rejects the removed %s method', (method, setting) => {
    expect(problemsFor(withOption(`      method: ${method}`, '      label: Key', setting))).toEqual(
      [expect.stringMatching(/^authentication\.options\.key\.method: /)],
    )
  })

  function withTokenUrl(url: string) {
    const option = withOption(
      ...signIn.slice(0, 3),
      `      token_url: "${url}"`,
      '      scopes: [read]',
    )
    return `${option}${siteInput}`
  }

  function withDocumentationUrl(url: string) {
    return withOption(
      '      method: bearer',
      '      label: Token',
      `      documentation_url: ${url}`,
    )
  }

  // These rules are shared with schema.json, so a check that stops working would let a
  // recipe through both.
  it.each([
    [
      'a request header Coral sets',
      `${minimal}request_headers:\n  authorization: Bearer x\n`,
      'request_headers.authorization: is set by Coral, not by a recipe',
    ],
    [
      'a credential header Coral sets',
      withHeaderKey('accept: {field: key}'),
      'authentication.options.key.headers.accept: cannot carry a credential',
    ],
    [
      'an authorization parameter Coral sets',
      withOption(...signIn, '      scopes: [read]', '      authorization_parameters: {state: x}'),
      'authentication.options.key.authorization_parameters.state: is set by Coral, not by a recipe',
    ],
    [
      'a constant password, which would keep a secret in stored configuration',
      withOption(
        '      method: basic_auth',
        '      label: Key',
        '      fields: {key: {label: Key}}',
        '      username: {field: key}',
        '      password: {literal: fixed}',
      ),
      'authentication.options.key.password: Invalid input',
    ],
    [
      'an empty fixed header value, which Coral refuses',
      withHeaderKey('x-api-key: {field: key}, x-mode: {literal: ""}'),
      'authentication.options.key.headers.x-mode.literal: Too small: expected string to have >=1 characters',
    ],
    [
      'a control character',
      minimal.replace('display_name: Example', 'display_name: "Ex\\tample"'),
      'display_name: must not contain control characters',
    ],
    [
      'a repeated scope',
      withOption(...signIn, '      scopes: [read, read]'),
      'authentication.options.key.scopes: must not repeat a scope',
    ],
    [
      'no authentication options',
      `${minimal}authentication:\n  options: {}\n`,
      'authentication.options: must offer between 1 and 16 options',
    ],
    [
      'a URL that is not http or https',
      withDocumentationUrl('ftp://docs.example.com/token'),
      'authentication.options.key.documentation_url: must be an http or https URL',
    ],
    [
      'a malformed URL',
      withDocumentationUrl('"https://docs example.com/token"'),
      'authentication.options.key.documentation_url: must be a valid URL',
    ],
    [
      'braces in a plain value',
      withHeaderKey('x-api-key: {field: key, prefix: "{token}"}'),
      'authentication.options.key.headers.x-api-key.prefix: must not contain braces',
    ],
    [
      'a name that is not lowercase',
      minimal.replace('id: example', 'id: Example'),
      'id: must be a lowercase name',
    ],
    [
      'a header name that is not lowercase',
      withHeaderKey('X-Api-Key: {field: key}'),
      'authentication.options.key.headers.X-Api-Key: must be a lowercase header name',
    ],
    [
      'a scope with a space',
      withOption(...signIn, '      scopes: ["read write"]'),
      'authentication.options.key.scopes.0: must not contain spaces',
    ],
    [
      'a repeated enum value',
      `${minimal}inputs:\n  region: {type: string, label: Region, enum: [us, us]}\n`,
      'inputs.region.enum: must not repeat a value',
    ],
    [
      'more than 16 options',
      `${minimal}authentication:\n  options: ${flowMap(17, (index) => `o${index}: {method: none, label: O}`)}\n`,
      'authentication.options: must offer between 1 and 16 options',
    ],
    [
      'more than 16 credential fields',
      withOption(
        '      method: header_credentials',
        '      label: Keys',
        `      fields: ${flowMap(17, (index) => `f${index}: {label: F}`)}`,
        '      headers: {x-key: {field: f0}}',
      ),
      'authentication.options.key.fields: must declare between 1 and 16 fields',
    ],
    [
      'more than 32 authorization parameters',
      withOption(
        ...signIn,
        '      scopes: [read]',
        `      authorization_parameters: ${flowMap(33, (index) => `p${index}: x`)}`,
      ),
      'authentication.options.key.authorization_parameters: must have at most 32 parameters',
    ],
    [
      'more than 64 request headers',
      `${minimal}request_headers: ${flowMap(65, (index) => `x-h${index}: v`)}\n`,
      'request_headers: must have at most 64 headers',
    ],
    [
      'more than 100 mapped values',
      [
        `${minimal}inputs:`,
        '  region: {type: string, label: Region}',
        'servers:',
        '  "#/servers":',
        '    declared_url: https://{site}.example.com',
        '    variables:',
        `      site: {input: region, values: ${flowMap(101, (index) => `v${index}: w`)}}`,
        '',
      ].join('\n'),
      'servers.#/servers.variables.site.values: must map between 1 and 100 values',
    ],
    [
      'empty field help',
      `${minimal}help:\n  servers:\n    "#/servers": {}\n`,
      'help.servers.#/servers: must not be empty',
    ],
    [
      'an icon path',
      `${minimal}icons:\n  light: images/example.svg\n`,
      'icons.light: must be the name of a .svg or .png file beside the recipe',
    ],
    [
      'a dark icon without a light one',
      `${minimal}icons:\n  dark: example-dark.svg\n`,
      'icons.light: Invalid input: expected string, received undefined',
    ],
    [
      'a declared URL with an input reference',
      `${minimal}${siteInput}servers:\n  "#/servers":\n    declared_url: "https://{{input.site}}.example.com"\n`,
      'servers.#/servers.declared_url: must be the exact declared template',
    ],
    [
      'a pointer that is not a servers declaration',
      `${minimal}servers:\n  "#/components":\n    base_url: https://api.example.com\n`,
      'servers.#/components: must point at a servers declaration',
    ],
    [
      'a template reference that is not an input',
      `${minimal}request_headers:\n  x-team: "{{secret.team}}"\n`,
      'request_headers.x-team: may only reference inputs as {{input.name}}',
    ],
    [
      'a URL template that does not start with http(s) or an input',
      withTokenUrl('ftp://{{input.site}}/token'),
      'authentication.options.key.token_url: Invalid input',
    ],
    [
      'a URL template that is neither a URL nor references an input',
      withTokenUrl('https://login example.com/token'),
      'authentication.options.key.token_url: Invalid input',
    ],
  ])('rejects %s', (_, text, problem) => {
    expect(problemsFor(text)).toContain(problem)
  })

  // The host-owned names must match whole names, not every header that contains one.
  it('accepts a request header that only contains a host-owned name', () => {
    expect(problemsFor(`${minimal}request_headers:\n  x-authorization: Bearer x\n`)).toEqual([])
  })

  // A URL check that trims or strips characters would hide them from the rules after it.
  it.each([
    ['a leading space', '" https://docs.example.com/token"'],
    ['a tab', '"https://docs.example.com/\\ttoken"'],
    ['a newline', '"https://docs.example.com/token\\n"'],
  ])('rejects a URL with %s', (_, url) => {
    expect(problemsFor(withDocumentationUrl(url))).toContainEqual(
      expect.stringMatching(/^authentication\.options\.key\.documentation_url: /),
    )
  })
})

function setupWith(
  configuration: Partial<NonNullable<SetupView['configuration']>> = {},
  kind: SetupView['kind'] = 'openapi',
) {
  return {
    kind,
    configuration: {
      confirmed: false,
      authenticationMethods: ['none', 'bearer', 'header', 'basic', 'query', 'oauth'],
      servers: [],
      ...configuration,
    },
  } as SetupView
}

function mcpRecipe(...client: string[]) {
  const result = parseRecipe(
    [
      'schema_version: 1',
      'id: example_mcp',
      'display_name: Example MCP',
      'source:',
      '  mcp:',
      '    url: https://mcp.example.com/mcp',
      'authentication:',
      '  options:',
      '    oauth:',
      '      method: oauth_discovery',
      '      label: Sign in',
      '      flow: authorization_code',
      ...client,
      '',
    ].join('\n'),
  )
  if (!result.ok) throw new Error(result.problems.join('\n'))
  return result.recipe
}

const hintContext = { callbackUrl: 'https://coral.example.com/provider-setup/oauth/callback' }

function recipeConfiguration(recipe: Recipe, setup: SetupView) {
  return configurationFor(recipe, setup, hintContext)
}

function mcpSetup(automaticClientAvailable: boolean) {
  return setupWith({ authenticationMethods: ['oauth'], automaticClientAvailable }, 'mcp')
}

describe('recipeConfiguration', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  const source = [
    'schema_version: 1',
    'id: example',
    'display_name: Example',
    'source:',
    '  openapi:',
    '    url: https://api.example.com/openapi.json',
    '',
  ].join('\n')

  function recipeFrom(...lines: string[]) {
    const result = parseRecipe(`${source}${lines.join('\n')}\n`)
    if (!result.ok) throw new Error(result.problems.join('\n'))
    return result.recipe
  }

  function tokenRecipe(...defaults: string[]) {
    return recipeFrom(
      'authentication:',
      ...defaults,
      '  options:',
      '    personal:',
      '      method: bearer',
      '      label: Personal token',
      '      documentation_url: https://docs.example.com/personal',
      '    service:',
      '      method: bearer',
      '      label: Service token',
      '      documentation_url: https://docs.example.com/service',
      '    none:',
      '      method: none',
      '      label: None',
    )
  }

  const baseValues = {
    displayName: 'Example',
    authentication: '' as const,
    headers: [],
    parameters: [],
    basic: {
      username: { source: 'literal' as const, value: '' },
      password: { source: 'literal' as const, value: '' },
    },
    credentialFields: {},
    authorizationUrl: '',
    tokenUrl: '',
    scopes: [],
    servers: [],
  }

  const signIn = [
    'inputs:',
    '  tenant:',
    '    type: string',
    '    label: Tenant',
    '    default: organizations',
    'authentication:',
    '  default: oauth',
    '  options:',
    '    oauth:',
    '      method: oauth_authorization_code',
    '      label: Sign in',
    '      authorization_url: https://login.example.com/{{input.tenant}}/authorize',
    '      token_url: https://login.example.com/{{input.tenant}}/token',
    '      client: {id: public-client}',
    '      scopes: [read]',
    '    token:',
    '      method: bearer',
    '      label: Token',
  ]

  it.for([[], [{ name: 'x-tenant', value: 'chosen' }]])(
    'preserves MCP acquisition headers, including removal: %j',
    (requestHeaders) => {
      const recipe = recipeFrom(
        'request_headers:',
        '  x-tenant: "{{input.tenant}}"',
        'inputs:',
        '  tenant:',
        '    type: string',
        '    label: Tenant',
      )
      const configuration = recipeConfiguration(
        recipe,
        setupWith(
          {
            values: { ...baseValues, requestHeaders },
          },
          'mcp',
        ),
      )!
      expect(
        filledRecipe(configuration, defaultAnswers(configuration)).values.requestHeaders,
      ).toEqual(requestHeaders)
      expect(configuration.inputs).toEqual([])
    },
  )

  it('pre-selects a sole option', () => {
    const recipe = recipeFrom(
      'authentication:',
      '  options:',
      '    api_key:',
      '      method: bearer',
      '      label: API key',
    )
    expect(recipeConfiguration(recipe, setupWith())?.values).toEqual({
      authentication: 'bearer',
    })
  })

  it('uses the default of several options, with its inputs filled in', () => {
    const configuration = recipeConfiguration(recipeFrom(...signIn), setupWith())!
    expect(configuration.inputs.map((input) => input.name)).toEqual(['tenant'])
    const filled = filledRecipe(configuration, defaultAnswers(configuration))
    expect(filled.values).toMatchObject({
      authentication: 'oauth',
      authorizationUrl: 'https://login.example.com/organizations/authorize',
      tokenUrl: 'https://login.example.com/organizations/token',
      scopes: ['read'],
    })
    expect(filled.application).toMatchObject({
      mode: 'manual',
      clientId: 'public-client',
    })
  })

  it('suggests only the offered methods that some option names', () => {
    const configuration = recipeConfiguration(
      recipeFrom(...signIn),
      setupWith({ authenticationMethods: ['none', 'bearer', 'header', 'oauth'] }),
    )
    expect(configuration?.suggestedMethods).toEqual(['bearer', 'oauth'])
  })

  it('pre-selects nothing when the server does not offer the default method', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const configuration = recipeConfiguration(
      recipeFrom(...signIn),
      setupWith({ authenticationMethods: ['bearer'] }),
    )
    expect(configuration?.values).toEqual({})
    expect(warn).toHaveBeenCalledOnce()
  })

  it('pre-fills header credentials with each binding and field', () => {
    const recipe = recipeFrom(
      'authentication:',
      '  options:',
      '    api_keys:',
      '      method: header_credentials',
      '      label: API and application keys',
      '      fields:',
      '        api_key: {label: API key}',
      '        application_key: {label: Application key, sensitive: false}',
      '      headers:',
      '        dd-api-key: {field: api_key}',
      '        authorization: {field: application_key, prefix: "Token token="}',
      '        x-client: {literal: coral}',
    )

    expect(recipeConfiguration(recipe, setupWith())?.values).toEqual({
      authentication: 'header',
      headers: [
        {
          name: 'authorization',
          source: 'field',
          field: 'application_key',
          prefix: 'Token token=',
        },
        { name: 'dd-api-key', source: 'field', field: 'api_key', prefix: '' },
        { name: 'x-client', source: 'literal', value: 'coral' },
      ],
      credentialFields: {
        api_key: { label: 'API key', description: '', sensitive: true },
        application_key: { label: 'Application key', description: '', sensitive: false },
      },
    })
  })

  it("pre-fills a constant Basic username such as Mailgun's api", () => {
    const recipe = recipeFrom(
      'authentication:',
      '  options:',
      '    api_key:',
      '      method: basic_auth',
      '      label: API key',
      '      fields: {api_key: {label: API key}}',
      '      username: {literal: api}',
      '      password: {field: api_key}',
    )

    expect(recipeConfiguration(recipe, setupWith())?.values).toEqual({
      authentication: 'basic',
      basic: {
        username: { source: 'literal', value: 'api' },
        password: { source: 'field', field: 'api_key' },
      },
      credentialFields: { api_key: { label: 'API key', description: '', sensitive: true } },
    })
  })

  it('pre-fills query credentials with a constant beside a key', () => {
    const recipe = recipeFrom(
      'authentication:',
      '  options:',
      '    api_key:',
      '      method: query_credentials',
      '      label: API key',
      '      fields: {key: {label: API key}}',
      '      parameters:',
      '        format: {literal: json}',
      '        api_key: {field: key}',
    )

    expect(recipeConfiguration(recipe, setupWith())?.values).toEqual({
      authentication: 'query',
      parameters: [
        { name: 'api_key', source: 'field', field: 'key' },
        { name: 'format', source: 'literal', value: 'json' },
      ],
      credentialFields: { key: { label: 'API key', description: '', sensitive: true } },
    })
  })

  // Coral takes no credentials in these forms from an MCP server, even when it offers the method.
  it.each([
    [
      'header credentials',
      'header',
      ['method: header_credentials', 'headers: {dd-api-key: {field: api_key}}'],
    ],
    [
      'Basic authentication',
      'basic',
      ['method: basic_auth', 'username: {literal: api}', 'password: {field: api_key}'],
    ],
    [
      'query credentials',
      'query',
      ['method: query_credentials', 'parameters: {key: {field: api_key}}'],
    ],
  ] as const)('pre-fills nothing for %s on an MCP server', (_, offered, option) => {
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    const recipe = recipeFrom(
      'authentication:',
      '  options:',
      '    api_key:',
      '      label: API key',
      '      fields: {api_key: {label: API key}}',
      ...option.map((line) => `      ${line}`),
    )

    expect(
      recipeConfiguration(recipe, setupWith({ authenticationMethods: [offered] }, 'mcp'))?.values,
    ).toEqual({})
  })

  it('pre-selects nothing when several options have no default', () => {
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    const recipe = recipeFrom(
      'authentication:',
      '  options:',
      '    first:',
      '      method: bearer',
      '      label: First',
      '    second:',
      '      method: none',
      '      label: Second',
    )
    expect(recipeConfiguration(recipe, setupWith())?.values).toEqual({})
  })

  it('fills non-secret request headers from recipe inputs independently of authentication', () => {
    const recipe = recipeFrom(
      'inputs:',
      '  tenant:',
      '    type: string',
      '    label: Tenant',
      'request_headers:',
      '  x-tenant: "{{input.tenant}}"',
      '  accept: application/json',
    )
    const configuration = recipeConfiguration(recipe, setupWith())!
    expect(configuration.inputs.map(({ name }) => name)).toEqual(['tenant'])
    expect(filledRecipe(configuration, { tenant: 'example' }).values.requestHeaders).toEqual([
      { name: 'accept', value: 'application/json' },
      { name: 'x-tenant', value: 'example' },
    ])
  })

  it('refills an unchanged recipe header after another header is edited', () => {
    const recipe = recipeFrom(
      'inputs:',
      '  tenant:',
      '    type: string',
      '    label: Tenant',
      'request_headers:',
      '  x-tenant: "{{input.tenant}}"',
      '  x-version: one',
    )
    const configuration = recipeConfiguration(recipe, setupWith())!
    const before = filledRecipe(configuration, { tenant: '' }).values
    const after = filledRecipe(configuration, { tenant: 'example' }).values
    const current = withRecipe(baseValues, before)
    current.requestHeaders = [
      ...current.requestHeaders!.map((header) =>
        header.name === 'x-version' ? { ...header, value: 'two' } : header,
      ),
      { name: 'x-extra', value: 'keep' },
    ]
    expect(refilled(current, baseValues, before, after).requestHeaders).toEqual([
      { name: 'x-tenant', value: 'example' },
      { name: 'x-version', value: 'two' },
      { name: 'x-extra', value: 'keep' },
    ])
  })

  // Scope tokens stay separate from their request encoding.
  const deviceSignIn = [
    'authentication:',
    '  options:',
    '    device:',
    '      method: oauth_device_code',
    '      label: Sign in',
    '      device_authorization_url: https://login.example.com/device',
    '      token_url: https://login.example.com/token',
    '      client: {id: public-app, authentication: none}',
    '      scopes: [read, write]',
  ]

  // Scope tokens stay separate from their request encoding.
  it.each([
    ['oauth', signIn, ['read']],
    ['device', deviceSignIn, ['read', 'write']],
  ] as const)('preserves %s scopes and comma request formatting', (method, lines, scopes) => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const recipe = recipeFrom(
      ...lines.flatMap((line) =>
        line.startsWith('      scopes:') ? [line, '      scope_format: {request: comma}'] : [line],
      ),
    )
    const configuration = recipeConfiguration(
      recipe,
      setupWith({ authenticationMethods: [method] }),
    )!
    expect(configuration.values).toMatchObject({
      authentication: method,
      scopes,
      scopeFormat: 'comma',
    })
    expect(warn).not.toHaveBeenCalled()
  })

  it('fills the device authorization URL and public application from a device recipe', () => {
    const configuration = recipeConfiguration(
      recipeFrom(...deviceSignIn),
      setupWith({ authenticationMethods: ['device'] }),
    )
    expect(configuration).toMatchObject({
      values: { authentication: 'device', authorizationUrl: 'https://login.example.com/device' },
      application: { clientId: 'public-app', authentication: 'none' },
    })
  })

  describe('OAuth application for an MCP server', () => {
    it('leaves an unavailable recipe device flow empty and names it without fallback', () => {
      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
      const recipe = mcpRecipe()
      const option = recipe.authentication!.options.oauth
      if (option.method !== 'oauth_discovery') throw new Error('discovery option')
      option.flow = 'device_code'
      recipe.authentication!.default = 'oauth'
      recipe.authentication!.options.token = { method: 'bearer', label: 'Token' }
      const configuration = recipeConfiguration(
        recipe,
        setupWith({ authenticationMethods: ['oauth', 'bearer'] }, 'mcp'),
      )!
      expect(configuration.values).toEqual({ authentication: '' })
      expect(configuration.application).toBeUndefined()
      expect(warn.mock.calls[0][0]).toContain('device_code')
    })

    it('selects discovered device code and suggests only that flow', () => {
      const recipe = mcpRecipe('      client: {id: public-client}')
      const option = recipe.authentication!.options.oauth
      if (option.method !== 'oauth_discovery') throw new Error('discovery option')
      option.flow = 'device_code'
      const configuration = recipeConfiguration(
        recipe,
        setupWith({ authenticationMethods: ['oauth', 'device'] }, 'mcp'),
      )!
      expect(configuration.values).toEqual({ authentication: 'device' })
      expect(configuration.suggestedMethods).toEqual(['device'])
      expect(configuration.application).toMatchObject({ clientId: 'public-client' })
    })

    it('uses the recipe client id even when Coral could register one', () => {
      const recipe = mcpRecipe('      client: {id: public-client}')
      expect(recipeConfiguration(recipe, mcpSetup(true))?.application).toMatchObject({
        mode: 'manual',
        clientId: 'public-client',
      })
    })

    // Without a client id, choosing your own application would leave an empty client id.
    it('lets Coral register the application when the recipe names no client id', () => {
      const recipe = mcpRecipe('      authorization_parameters: {prompt: consent}')
      expect(recipeConfiguration(recipe, mcpSetup(true))?.application).toEqual({
        mode: 'automatic',
        clientId: '',
        authentication: 'none',
        pkce: 's256',
        parameters: 'prompt=consent',
      })
    })

    // Recipe scopes override the discovery suggestion, without changing endpoints.
    // An empty list is an override too: it must not fall back to discovery.
    it.for([[['read']], [[]]])('fills in the requested discovery scopes: %j', ([scopes]) => {
      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
      const configuration = recipeConfiguration(
        mcpRecipe(`      scopes: ${JSON.stringify(scopes)}`),
        mcpSetup(true),
      )
      expect(configuration?.values).toMatchObject({
        authentication: 'oauth',
        scopes,
        scopeFormat: 'space',
      })
      expect(warn).not.toHaveBeenCalled()
    })

    const generator = '      client_config_hint_generator: slack_app_create_link'
    it.for([
      { name: 'no hint without a generator', recipe: () => mcpRecipe(), hint: undefined },
      {
        name: 'the hint its generator writes',
        recipe: () => mcpRecipe(generator),
        hint: CLIENT_CONFIG_HINT_GENERATORS.slack_app_create_link(hintContext),
      },
      // The hint follows the option the recipe fills in, not any option with a generator.
      {
        name: 'no hint from an option the recipe does not fill in',
        recipe: () =>
          mcpRecipe(
            '    hinted:',
            '      method: oauth_discovery',
            '      label: Sign in with a hint',
            '      flow: authorization_code',
            generator,
            '  default: oauth',
          ),
        hint: undefined,
      },
    ])('adds $name', ({ recipe, hint }) => {
      const configuration = recipeConfiguration(recipe(), mcpSetup(false))
      expect(configuration?.clientConfigHintMarkdown).toBe(hint)
    })

    it('asks for your own application when Coral cannot register one', () => {
      const recipe = mcpRecipe('      authorization_parameters: {prompt: consent}')
      expect(recipeConfiguration(recipe, mcpSetup(false))?.application).toMatchObject({
        mode: 'manual',
        parameters: 'prompt=consent',
      })
    })
  })

  it('fills only a first configuration', () => {
    const recipe = recipeFrom(...signIn)
    expect(recipeConfiguration(recipe, setupWith({ confirmed: true }))).toBeNull()
  })

  describe('servers', () => {
    const servers = [
      'inputs:',
      '  region:',
      '    type: string',
      '    label: Region',
      '    default: eu',
      '    enum: [us, eu]',
      '  site:',
      '    type: url',
      '    label: Site',
      'servers:',
      '  "#/servers":',
      '    declared_url: https://{site}.example.com',
      '    variables:',
      '      site:',
      '        input: region',
      '        values: {us: us-east, eu: eu-west}',
      '  "#/paths/~1logs/servers":',
      '    base_url: "{{input.site}}/logs"',
      '  "#/paths/~1missing/servers":',
      '    base_url: https://missing.example.com',
    ]
    const review = {
      servers: [
        {
          declaration: '#/servers',
          tools: [],
          options: [
            {
              url: 'https://{site}.example.com',
              variables: [
                {
                  name: 'site',
                  defaultValue: 'us-east',
                  enumerated: true,
                  values: ['us-east', 'eu-west'],
                },
              ],
            },
          ],
        },
        {
          declaration: '#/paths/~1logs/servers',
          tools: [],
          options: [{ url: 'https://logs.example.com', variables: [] }],
        },
      ],
    }

    it('maps declarations the document has, and fills them from inputs', () => {
      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
      const configuration = recipeConfiguration(recipeFrom(...servers), setupWith(review))!
      expect(warn.mock.calls[0][0]).toMatch(/~1missing/)
      expect(configuration.values.servers?.map((server) => server.declaration)).toEqual([
        '#/servers',
        '#/paths/~1logs/servers',
      ])
      expect(filledRecipe(configuration, defaultAnswers(configuration)).values.servers).toEqual([
        {
          declaration: '#/servers',
          url: 'https://{site}.example.com',
          custom: false,
          variables: { site: 'eu-west' },
        },
        // No site answer yet, so the custom URL stays empty.
        {
          declaration: '#/paths/~1logs/servers',
          url: '',
          custom: true,
          variables: {},
        },
      ])
      const answered = filledRecipe(configuration, {
        region: 'us',
        site: 'https://acme.example.com',
      })
      expect(answered.values.servers).toEqual([
        {
          declaration: '#/servers',
          url: 'https://{site}.example.com',
          custom: false,
          variables: { site: 'us-east' },
        },
        {
          declaration: '#/paths/~1logs/servers',
          url: 'https://acme.example.com/logs',
          custom: true,
          variables: {},
        },
      ])
    })

    // A declared URL with an empty variable would send a half-built server.
    it.each([
      [
        'a text answer',
        ['inputs:', '  organization:', '    type: string', '    label: Organization'],
        ['      site: "{{input.organization}}"'],
        { organization: 'acme' },
        'acme',
      ],
      [
        'a choice',
        ['inputs:', '  region:', '    type: string', '    label: Region', '    enum: [us, eu]'],
        ['      site:', '        input: region', '        values: {us: us-east, eu: eu-west}'],
        { region: 'eu' },
        'eu-west',
      ],
    ])('keeps the document server while %s is missing', (_, inputs, site, answers, filledIn) => {
      const recipe = recipeFrom(
        ...inputs,
        'servers:',
        '  "#/servers":',
        '    declared_url: https://{site}.example.com',
        '    variables:',
        ...site,
      )
      const configuration = recipeConfiguration(recipe, setupWith(review))!
      const documentServer = {
        declaration: '#/servers',
        url: 'https://{site}.example.com',
        custom: false,
        variables: { site: 'us-east' },
      }
      const base = { ...baseValues, servers: [documentServer] }
      const unanswered = filledRecipe(configuration, defaultAnswers(configuration)).values
      expect(withRecipe(base, unanswered).servers).toEqual([documentServer])
      const answered = filledRecipe(configuration, answers).values
      expect(refilled(withRecipe(base, unanswered), base, unanswered, answered).servers).toEqual([
        { ...documentServer, variables: { site: filledIn } },
      ])
    })

    // A free-text answer can carry spaces, as a text reference already allows.
    it('trims an answer before looking it up in a value map', () => {
      const recipe = recipeFrom(
        'inputs:',
        '  region:',
        '    type: string',
        '    label: Region',
        'servers:',
        '  "#/servers":',
        '    declared_url: https://{site}.example.com',
        '    variables:',
        '      site:',
        '        input: region',
        '        values: {us: us-east, eu: eu-west}',
      )
      const configuration = recipeConfiguration(recipe, setupWith(review))!
      expect(filledRecipe(configuration, { region: ' eu ' }).values.servers).toEqual([
        {
          declaration: '#/servers',
          url: 'https://{site}.example.com',
          custom: false,
          variables: { site: 'eu-west' },
        },
      ])
    })

    it('skips a declared URL the document does not declare', () => {
      vi.spyOn(console, 'warn').mockImplementation(() => {})
      const recipe = recipeFrom(
        ...servers.map((line) =>
          line.includes('declared_url') ? '    declared_url: https://other.example.com' : line,
        ),
      )
      const configuration = recipeConfiguration(recipe, setupWith(review))!
      expect(configuration.values.servers?.map((server) => server.declaration)).toEqual([
        '#/paths/~1logs/servers',
      ])
    })
  })

  it('re-fills only values the person has not changed', () => {
    const configuration = recipeConfiguration(recipeFrom(...signIn), setupWith())!
    const before = filledRecipe(configuration, {
      tenant: 'organizations',
    }).values
    const after = filledRecipe(configuration, { tenant: 'contoso' }).values
    const start = withRecipe(baseValues, before)
    const edited = {
      ...start,
      tokenUrl: 'https://login.example.com/custom/token',
    }
    const next = refilled(edited, baseValues, before, after)
    expect(next.authorizationUrl).toBe('https://login.example.com/contoso/authorize')
    expect(next.tokenUrl).toBe('https://login.example.com/custom/token')
  })

  describe('recipeDocumentationUrl', () => {
    it("links to the default option's docs when several options use the method", () => {
      expect(recipeDocumentationUrl(tokenRecipe('  default: service'), 'bearer')).toBe(
        'https://docs.example.com/service',
      )
    })

    it('links to the first option for the method when the default uses another', () => {
      expect(recipeDocumentationUrl(tokenRecipe('  default: none'), 'bearer')).toBe(
        'https://docs.example.com/personal',
      )
    })

    it('links nowhere for a method with no documented option, or no method', () => {
      expect(recipeDocumentationUrl(tokenRecipe(), 'none')).toBeNull()
      expect(recipeDocumentationUrl(tokenRecipe(), 'oauth')).toBeNull()
      expect(recipeDocumentationUrl(tokenRecipe(), null)).toBeNull()
    })
  })
})
