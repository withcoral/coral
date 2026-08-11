// Source presets: APIs Coral can already talk to, advertised in the catalog
// before anyone has installed them. Each entry is a spec URL the source
// discovery flow can fetch and classify, so clicking a preset card lands the
// user in the create flow with the URL prefilled and only credentials left to
// supply.
//
// These are NOT bundled sources. A bundled source ships a curated manifest under
// sources/core with typed columns, per-table guides and pagination; a preset
// carries a URL and nothing else, and the manifest is derived at install time.
// Bundled always wins: catalogEntries() drops any preset whose name is already in
// the compiled catalog or already installed, so promoting a manifest into
// sources/core makes its preset card disappear on its own.
//
// Only `openapi` and `mcp` belong here — those are the two surfaces the create
// flow understands (see SurfaceType in views/sources/source-create.tsx). Google
// Discovery documents and GraphQL endpoints cannot be created this way.

export interface SourcePreset {
  /** Canonical source name. Must match the name a manifest would use. */
  name: string
  /** One-line summary, shown on the card. */
  description: string
  /** HTTPS URL of the OpenAPI descriptor, or the MCP server endpoint. */
  specUrl: string
  surfaceType: 'openapi' | 'mcp'
}

export const SOURCE_PRESETS: readonly SourcePreset[] = [
  {
    name: 'asana',
    description: 'Query tasks, projects, sections, teams, and workspaces from Asana.',
    specUrl:
      'https://raw.githubusercontent.com/APIs-guru/openapi-directory/main/APIs/asana.com/1.0/openapi.yaml',
    surfaceType: 'openapi',
  },
  {
    name: 'axiom',
    // Axiom's published OpenAPI descriptor (axiom.co/docs/restapi/versions/v2.json)
    // now 404s, so this points at their MCP server instead.
    description: 'Query datasets, monitors, and saved queries through the Axiom MCP server.',
    specUrl: 'https://mcp.axiom.co/mcp',
    surfaceType: 'mcp',
  },
  {
    name: 'browserbase',
    description: 'Drive headless browser sessions through the Browserbase MCP server.',
    specUrl: 'https://mcp.browserbase.com/mcp',
    surfaceType: 'mcp',
  },
  {
    name: 'cloudflare',
    description: 'Query zones, DNS records, workers, and analytics from Cloudflare.',
    specUrl: 'https://raw.githubusercontent.com/cloudflare/api-schemas/main/openapi.json',
    surfaceType: 'openapi',
  },
  {
    name: 'context7',
    description: 'Look up library and framework documentation through the Context7 MCP server.',
    specUrl: 'https://mcp.context7.com/mcp',
    surfaceType: 'mcp',
  },
  {
    name: 'deepwiki',
    description: 'Ask questions about public repositories through the DeepWiki MCP server.',
    specUrl: 'https://mcp.deepwiki.com/mcp',
    surfaceType: 'mcp',
  },
  {
    name: 'digitalocean',
    description:
      'Query droplets, Kubernetes clusters, databases, and networking from DigitalOcean.',
    specUrl:
      'https://raw.githubusercontent.com/digitalocean/openapi/main/specification/DigitalOcean-public.v2.yaml',
    surfaceType: 'openapi',
  },
  {
    name: 'exa',
    description: 'Run web searches and retrieve page contents from Exa.',
    specUrl:
      'https://raw.githubusercontent.com/exa-labs/openapi-spec/refs/heads/master/exa-openapi-spec.yaml',
    surfaceType: 'openapi',
  },
  {
    name: 'figma',
    description: 'Query files, comments, components, variables, projects, and webhooks from Figma.',
    specUrl:
      'https://raw.githubusercontent.com/figma/rest-api-spec/refs/heads/main/openapi/openapi.yaml',
    surfaceType: 'openapi',
  },
  {
    name: 'firecrawl',
    description: 'Crawl and scrape pages through the Firecrawl MCP server.',
    specUrl: 'https://mcp.firecrawl.dev/mcp',
    surfaceType: 'mcp',
  },
  {
    name: 'neon',
    description: 'Query projects, branches, databases, and endpoints from Neon.',
    specUrl: 'https://neon.tech/api_spec/release/v2.json',
    surfaceType: 'openapi',
  },
  {
    name: 'openai',
    description: 'Query models, files, batches, and fine-tuning jobs from OpenAI.',
    specUrl: 'https://app.stainless.com/api/spec/documented/openai/openapi.documented.yml',
    surfaceType: 'openapi',
  },
  {
    name: 'resend',
    description: 'Query sent emails, domains, audiences, and contacts from Resend.',
    specUrl: 'https://raw.githubusercontent.com/resend/resend-openapi/main/resend.yaml',
    surfaceType: 'openapi',
  },
  {
    name: 'spotify',
    description: 'Query tracks, albums, artists, playlists, and listening history from Spotify.',
    specUrl:
      'https://raw.githubusercontent.com/sonallux/spotify-web-api/refs/heads/main/official-spotify-open-api.yml',
    surfaceType: 'openapi',
  },
  {
    name: 'twilio',
    description: 'Query messages, calls, phone numbers, and recordings from Twilio.',
    specUrl:
      'https://raw.githubusercontent.com/twilio/twilio-oai/main/spec/json/twilio_api_v2010.json',
    surfaceType: 'openapi',
  },
  {
    name: 'val_town',
    description: 'Query vals, runs, blobs, and HTTP endpoints from Val Town.',
    specUrl: 'https://api.val.town/openapi.json',
    surfaceType: 'openapi',
  },
  {
    name: 'vercel',
    description: 'Query deployments, projects, domains, and logs from Vercel.',
    specUrl: 'https://openapi.vercel.sh',
    surfaceType: 'openapi',
  },
] as const

/**
 * Query contract between a preset card and the create flow. Kept here so the
 * link builder and the route parser can never drift apart.
 */
const SPEC_PARAM = 'spec'
const KIND_PARAM = 'kind'
const NAME_PARAM = 'name'

/** Builds the create-flow link for one preset. */
export function sourceCreatePath(
  installPath: string,
  entry: { name: string; preset: { specUrl: string; surfaceType: string } },
): string {
  const params = new URLSearchParams({
    [SPEC_PARAM]: entry.preset.specUrl,
    [KIND_PARAM]: entry.preset.surfaceType,
    [NAME_PARAM]: entry.name,
  })
  return `${installPath}?${params.toString()}`
}

export interface SourceCreatePrefill {
  name?: string
  surfaceType?: SourcePreset['surfaceType']
  url: string
}

/**
 * Reads a prefill out of a create-flow query string. Takes the params rather than
 * a whole URL because that is all it reads, which lets the route parse them on the
 * client instead of paying for a loader. Returns null unless the spec is an HTTPS
 * URL — discovery rejects anything else anyway, and the query string is untrusted
 * input, so an unusable value should leave the flow empty rather than seed it with
 * junk. An unrecognised `kind` is dropped instead of trusted, and the create flow
 * falls back to its own default.
 */
export function sourceCreatePrefill(params: URLSearchParams): SourceCreatePrefill | null {
  const spec = params.get(SPEC_PARAM)?.trim()
  if (!spec || !spec.startsWith('https://')) return null

  const kind = params.get(KIND_PARAM)?.trim()
  const name = params.get(NAME_PARAM)?.trim()

  return {
    ...(name ? { name } : {}),
    ...(kind === 'openapi' || kind === 'mcp' ? { surfaceType: kind } : {}),
    url: spec,
  }
}
