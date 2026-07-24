import { afterEach, describe, expect, it, vi } from 'vitest'

import { inspectSourceDocument, loader } from './source-discovery'

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
        }),
      ),
    ).toEqual({
      auth: { kind: 'bearer', label: 'Bearer token' },
      description: 'Weather observations and forecasts',
      format: 'openapi-json',
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
      title: 'Weather API',
    })
  })

  it('reports documents without an OpenAPI version as unknown', () => {
    expect(inspectSourceDocument('{"name":"not-openapi"}')).toEqual({
      auth: { kind: 'unknown', label: '' },
      description: '',
      format: 'unknown',
      title: '',
    })
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
      status: 'success',
      url: 'https://status.example/openapi.json',
    })
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
