import { afterEach, describe, expect, it, vi } from 'vitest'

import { inspectSourceDocument, loader } from './source-discovery'

const serverlessSpec = JSON.stringify({
  info: { title: 'Lords Votes API' },
  openapi: '3.0.1',
  paths: { '/data/Divisions/groupedbyparty': {}, '/data/Divisions/{divisionId}': {} },
})

/** A response as `fetch` returns it after following a redirect: `url` is the final one. */
const servedFrom = (response: Response, url: string) =>
  Object.defineProperty(response, 'url', { value: url })

const discoverRequest = (url: string, signal?: AbortSignal) =>
  new Request(
    `http://reef.test/workspaces/default/sources/discover?url=${encodeURIComponent(url)}`,
    signal ? { signal } : undefined,
  )

describe('source discovery', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('extracts source details from OpenAPI JSON', () => {
    expect(
      inspectSourceDocument(
        JSON.stringify({
          components: {
            securitySchemes: {
              bearerAuth: { scheme: 'bearer', type: 'http' },
            },
          },
          info: { description: 'Weather observations and forecasts', title: 'Weather API' },
          openapi: '3.1.0',
          security: [{ bearerAuth: [] }],
          servers: [{ url: 'https://weather.example/v1' }],
        }),
      ),
    ).toEqual({
      auth: { kind: 'bearer', label: 'Bearer token' },
      description: 'Weather observations and forecasts',
      format: 'openapi-json',
      probePath: '',
      serverUrl: 'https://weather.example/v1',
      title: 'Weather API',
    })
  })

  it('extracts source details from OpenAPI YAML', () => {
    expect(
      inspectSourceDocument(`openapi: 3.1.0
info:
  title: Weather API
  description: >
    Weather observations
    and forecasts
servers:
  - url: https://weather.example/v1
    description: Production
paths: {}
components:
  securitySchemes:
    apiKey:
      type: apiKey
      in: header
      name: X-Api-Key
`),
    ).toEqual({
      auth: { headerName: 'X-Api-Key', kind: 'header', label: 'Header X-Api-Key' },
      description: 'Weather observations and forecasts',
      format: 'openapi-yaml',
      probePath: '',
      serverUrl: 'https://weather.example/v1',
      title: 'Weather API',
    })
  })

  it('reports documents without an OpenAPI version as unknown', () => {
    expect(inspectSourceDocument('{"name":"not-openapi"}')).toEqual({
      auth: { kind: 'unknown', label: '' },
      description: '',
      format: 'unknown',
      probePath: '',
      serverUrl: '',
      title: '',
    })
  })

  it('resolves server URL variables from their declared defaults', () => {
    expect(
      inspectSourceDocument(
        JSON.stringify({
          info: { title: 'Region API' },
          openapi: '3.1.0',
          servers: [
            { url: 'https://{region}.example.com/{version}', variables: { region: {} } },
            {
              url: 'https://{region}.example.com/{version}',
              variables: { region: { default: 'eu' }, version: { default: 'v2' } },
            },
          ],
        }),
      ).serverUrl,
    ).toBe('https://eu.example.com/v2')
  })

  it('maps OAuth security schemes to a bearer credential', () => {
    expect(
      inspectSourceDocument(
        JSON.stringify({
          components: { securitySchemes: { oauth: { flows: {}, type: 'oauth2' } } },
          info: { title: 'OAuth API' },
          openapi: '3.1.0',
          security: [{ oauth: [] }],
        }),
      ).auth,
    ).toEqual({ kind: 'bearer', label: 'OAuth 2.0 bearer token' })
  })

  it('reports unsupported authentication without selecting an incompatible credential', () => {
    expect(
      inspectSourceDocument(
        JSON.stringify({
          components: {
            securitySchemes: { queryKey: { in: 'query', name: 'api_key', type: 'apiKey' } },
          },
          info: { title: 'Query API' },
          openapi: '3.1.0',
          security: [{ queryKey: [] }],
        }),
      ).auth,
    ).toEqual({ kind: 'unsupported', label: 'query API key' })
  })

  it('loads the URL and returns a query-safe source name', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            info: { description: 'Status checks', title: '123 Status API' },
            openapi: '3.0.3',
            security: [],
            servers: [{ url: 'https://status.example/api' }],
          }),
          { status: 200 },
        ),
      ),
    )
    const request = new Request(
      'http://reef.test/workspaces/default/sources/discover?url=https%3A%2F%2Fstatus.example%2Fopenapi.json',
    )

    await expect(loader({ request } as Parameters<typeof loader>[0])).resolves.toEqual({
      auth: { kind: 'none', label: 'No authentication' },
      description: 'Status checks',
      format: 'openapi-json',
      name: 'source_123_status_api',
      serverUrl: 'https://status.example/api',
      status: 'success',
      url: 'https://status.example/openapi.json',
    })
  })

  it('resolves a relative server URL against the document location', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            info: { title: 'Status API' },
            openapi: '3.0.3',
            servers: [{ url: '/v1' }],
          }),
          { status: 200 },
        ),
      ),
    )
    const request = new Request(
      'http://reef.test/workspaces/default/sources/discover?url=https%3A%2F%2Fstatus.example%2Fdocs%2Fopenapi.json',
    )

    await expect(loader({ request } as Parameters<typeof loader>[0])).resolves.toMatchObject({
      serverUrl: 'https://status.example/v1',
    })
  })

  it('resolves a relative server URL against a redirected document location', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        servedFrom(
          new Response(
            JSON.stringify({
              info: { title: 'Status API' },
              openapi: '3.0.3',
              servers: [{ url: '/v1' }],
            }),
            { status: 200 },
          ),
          'https://api.status.example/openapi.json',
        ),
      ),
    )

    await expect(
      loader({
        request: discoverRequest('https://docs.status.example/openapi.json'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toMatchObject({ serverUrl: 'https://api.status.example/v1' })
  })

  it('probes the redirected document location rather than the requested host', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        servedFrom(
          new Response(serverlessSpec, { status: 200 }),
          'https://api.status.example/openapi.json',
        ),
      )
      .mockResolvedValueOnce(new Response('[]', { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      loader({
        request: discoverRequest('https://docs.status.example/openapi.json'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toMatchObject({ serverUrl: 'https://api.status.example' })

    expect(fetchMock.mock.calls[1][0].toString()).toBe(
      'https://api.status.example/data/Divisions/groupedbyparty',
    )
  })

  it('derives the server URL from the fetch origin when a probed path is served', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(serverlessSpec, { status: 200 }))
      .mockResolvedValueOnce(new Response('[]', { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      loader({
        request: discoverRequest('https://lordsvotes-api.parliament.uk/swagger/v1/swagger.json'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toMatchObject({ serverUrl: 'https://lordsvotes-api.parliament.uk' })

    expect(fetchMock.mock.calls[1][0].toString()).toBe(
      'https://lordsvotes-api.parliament.uk/data/Divisions/groupedbyparty',
    )
  })

  it('keeps a credentialed API when the probe asks for authentication', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(new Response(serverlessSpec, { status: 200 }))
        .mockResolvedValueOnce(new Response('', { status: 401 })),
    )

    await expect(
      loader({
        request: discoverRequest('https://lordsvotes-api.parliament.uk/swagger/v1/swagger.json'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toMatchObject({ serverUrl: 'https://lordsvotes-api.parliament.uk' })
  })

  it('leaves the server URL empty when the probed path is not served', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(new Response(serverlessSpec, { status: 200 }))
        .mockResolvedValueOnce(new Response('', { status: 404 })),
    )

    await expect(
      loader({
        request: discoverRequest('https://raw.githubusercontent.com/org/repo/openapi.json'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toMatchObject({ serverUrl: '' })
  })

  it('cancels the probe when the discovery request is aborted', async () => {
    const controller = new AbortController()
    const probeSignals: (AbortSignal | undefined)[] = []
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(serverlessSpec, { status: 200 }))
      .mockImplementationOnce((_url: URL, init: RequestInit) => {
        probeSignals.push(init.signal ?? undefined)
        controller.abort()
        return Promise.reject(new DOMException('The operation was aborted', 'AbortError'))
      })
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      loader({
        request: discoverRequest('https://api.status.example/openapi.json', controller.signal),
      } as Parameters<typeof loader>[0]),
    ).rejects.toThrow()

    expect(probeSignals[0]?.aborted).toBe(true)
  })

  it('does not probe a path key that resolves to another host', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          info: { title: 'Lords Votes API' },
          openapi: '3.0.1',
          paths: { '//attacker.example/data': {} },
        }),
        { status: 200 },
      ),
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      loader({
        request: discoverRequest('https://raw.githubusercontent.com/org/repo/openapi.json'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toMatchObject({ serverUrl: '' })

    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('does not probe when a declared server URL cannot be resolved', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          info: { title: 'Region API' },
          openapi: '3.0.3',
          paths: { '/status': {} },
          servers: [{ url: 'https://{region}.example.com' }],
        }),
        { status: 200 },
      ),
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      loader({
        request: discoverRequest('https://docs.example.com/openapi.json'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toMatchObject({ serverUrl: '' })

    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('does not probe when the document declares its own servers', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          info: { title: 'Status API' },
          openapi: '3.0.3',
          paths: { '/status': {} },
          servers: [{ url: 'https://status.example/api' }],
        }),
        { status: 200 },
      ),
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      loader({
        request: discoverRequest('https://mirror.example/specs/status.json'),
      } as Parameters<typeof loader>[0]),
    ).resolves.toMatchObject({ serverUrl: 'https://status.example/api' })

    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('rejects non-HTTPS discovery URLs without fetching them', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    const request = new Request(
      'http://reef.test/workspaces/default/sources/discover?url=http%3A%2F%2Flocalhost%2Fopenapi.json',
    )

    await expect(loader({ request } as Parameters<typeof loader>[0])).resolves.toEqual({
      message: 'Source discovery requires an HTTPS URL',
      status: 'error',
      url: 'http://localhost/openapi.json',
    })
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('detects an HTTP-rejecting /mcp endpoint as an MCP server', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 405 })))
    const request = new Request(
      'http://reef.test/workspaces/default/sources/discover?url=https%3A%2F%2Ftools.example%2Fmcp',
    )

    await expect(loader({ request } as Parameters<typeof loader>[0])).resolves.toEqual({
      auth: { kind: 'unknown', label: '' },
      description: '',
      format: 'mcp',
      inspectionError: 'The URL returned HTTP 405',
      name: 'mcp',
      serverUrl: '',
      status: 'success',
      url: 'https://tools.example/mcp',
    })
  })

  it('detects an /sse endpoint with a trailing slash and query as an MCP server', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(new Response('event: endpoint', { status: 200 })),
    )
    const request = new Request(
      'http://reef.test/workspaces/default/sources/discover?url=https%3A%2F%2Ftools.example%2Fevents%2Fsse%2F%3Ftoken%3Dsecret',
    )

    await expect(loader({ request } as Parameters<typeof loader>[0])).resolves.toEqual({
      auth: { kind: 'unknown', label: '' },
      description: '',
      format: 'mcp',
      name: 'sse',
      serverUrl: '',
      status: 'success',
      url: 'https://tools.example/events/sse/?token=secret',
    })
  })

  it('keeps OpenAPI detection authoritative for an /mcp URL', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ info: { title: 'Tools API' }, openapi: '3.1.0' }), {
          status: 200,
        }),
      ),
    )
    const request = new Request(
      'http://reef.test/workspaces/default/sources/discover?url=https%3A%2F%2Ftools.example%2Fmcp',
    )

    await expect(loader({ request } as Parameters<typeof loader>[0])).resolves.toMatchObject({
      format: 'openapi-json',
      name: 'tools_api',
    })
  })
})
