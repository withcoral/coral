import type { Route } from './+types/source-discovery'

const MAX_DESCRIPTOR_SIZE_MB = 64
// Keep this aligned with coral-app's DSL v4 materialization limit.
const MAX_DESCRIPTOR_BYTES = MAX_DESCRIPTOR_SIZE_MB * 1024 * 1024

export type SourceDocumentFormat = 'mcp' | 'openapi-json' | 'openapi-yaml' | 'unknown'

/** A method the credentials step offers, so one a detected scheme can be answered with. */
export type SourceAuthChoice = 'bearer' | 'header' | 'none'

export type SourceDetectedAuth =
  | { kind: 'unknown' }
  | { kind: 'unsupported'; label: string }
  | {
      /** Header names needed together by the preselected alternative. */
      headerNames: string[]
      kind: SourceAuthChoice
      /** Every usable method the document accepts, in declaration order. */
      kinds: SourceAuthChoice[]
    }

export type SourceDiscoveryData =
  | {
      auth: SourceDetectedAuth
      description: string
      format: SourceDocumentFormat
      inspectionError?: string
      name: string
      serverUrl: string
      status: 'success'
      title: string
      url: string
    }
  | {
      message: string
      status: 'error'
      url: string
    }

export async function loader({ request }: Route.LoaderArgs): Promise<SourceDiscoveryData> {
  const rawUrl = new URL(request.url).searchParams.get('url')?.trim() ?? ''
  if (!rawUrl) return discoveryError(rawUrl, 'Enter a source URL')

  let sourceUrl: URL
  try {
    sourceUrl = new URL(rawUrl)
  } catch {
    return discoveryError(rawUrl, 'Enter a valid HTTPS URL')
  }
  if (sourceUrl.protocol !== 'https:') {
    return discoveryError(rawUrl, 'Source discovery requires an HTTPS URL')
  }

  const signal = AbortSignal.any([request.signal, AbortSignal.timeout(10_000)])
  try {
    const response = await fetch(sourceUrl, {
      headers: {
        accept: 'application/json, application/yaml, text/yaml, text/plain, */*;q=0.1',
      },
      signal,
    })
    if (!response.ok) {
      return discoveredSource(rawUrl, sourceUrl, {
        auth: unknownAuth(),
        description: '',
        format: 'unknown',
        inspectionError: `The URL returned HTTP ${response.status}`,
        serverUrl: '',
        title: '',
      })
    }

    const document = await responseTextWithinLimit(response)
    const metadata = inspectSourceDocument(document)
    const documentUrl = documentLocation(response, sourceUrl)
    const serverUrl =
      absoluteServerUrl(metadata.serverUrl, documentUrl) ||
      (await derivedServerUrl(documentUrl, metadata.probePath, signal))
    // A probe that times out costs only the base URL, but a caller who navigated
    // away gets no answer at all.
    request.signal.throwIfAborted()
    return discoveredSource(rawUrl, sourceUrl, { ...metadata, serverUrl })
  } catch (error) {
    if (request.signal.aborted) throw error
    let inspectionError = 'The source URL could not be loaded'
    if (signal.aborted) inspectionError = 'Source discovery timed out'
    else if (error instanceof Error) inspectionError = error.message
    return discoveredSource(rawUrl, sourceUrl, {
      auth: unknownAuth(),
      description: '',
      format: 'unknown',
      inspectionError,
      serverUrl: '',
      title: '',
    })
  }
}

export function inspectSourceDocument(document: string): {
  auth: SourceDetectedAuth
  description: string
  format: SourceDocumentFormat
  probePath: string
  serverUrl: string
  title: string
} {
  const json = parseJsonObject(document)
  if (json && hasOpenApiVersion(json)) {
    const info = objectValue(json.info)
    const servers = Array.isArray(json.servers) ? json.servers : []
    return {
      auth: inspectJsonAuth(json),
      description: stringValue(info?.description),
      format: 'openapi-json',
      // OpenAPI 3 defines an absent or empty `servers` block as one entry with a
      // url of `/`, and a relative server url as relative to where the document
      // is served. So that case, and only that case, gets a base URL derived from
      // the document's own location. A declared entry that will not resolve points
      // somewhere we cannot infer, and is left for the user to fill in.
      //
      // Swagger 2 has no `servers` at all — it spells the base URL out in `host`,
      // `basePath` and `schemes` — so an empty block there says nothing about
      // where the API lives, and a derived origin would drop whatever `basePath`
      // declared. Those documents are left for the user to fill in.
      probePath: isOpenApi3(json) && servers.length === 0 ? firstConcretePath(json) : '',
      serverUrl: resolvedServerUrl(servers),
      title: stringValue(info?.title),
    }
  }

  // The YAML scanner is line-based, so a YAML document declaring no servers
  // leaves the field empty for the user to fill rather than being probed.
  const yaml = inspectOpenApiYaml(document)
  if (yaml) return { ...yaml, format: 'openapi-yaml', probePath: '' }

  return {
    auth: unknownAuth(),
    description: '',
    format: 'unknown',
    probePath: '',
    serverUrl: '',
    title: '',
  }
}

/**
 * An operation path that can be requested as written, for checking whether a host
 * serves this API. A `{placeholder}` left in the URL would 404 on its own merits.
 */
function firstConcretePath(document: Record<string, unknown>): string {
  const paths = objectValue(document.paths)
  return Object.keys(paths ?? {}).find((path) => path.startsWith('/') && !path.includes('{')) ?? ''
}

/**
 * First `servers[]` entry whose URL resolves, mirroring coral-spec's
 * `openapi_server_url`: `{name}` placeholders come from `variables.<name>.default`,
 * and an entry with an unresolvable placeholder is skipped rather than used raw.
 */
function resolvedServerUrl(servers: unknown[]): string {
  for (const entry of servers) {
    const server = objectValue(entry)
    const url = stringValue(server?.url)
    if (!url) continue
    const resolved = resolveServerUrl(url, objectValue(server?.variables))
    if (resolved) return resolved
  }
  return ''
}

function resolveServerUrl(url: string, variables: Record<string, unknown> | undefined): string {
  const resolved = url.replace(/\{([^{}]*)\}/g, (placeholder, name: string) => {
    return stringValue(objectValue(variables?.[name])?.default) || placeholder
  })
  return resolved.includes('{') ? '' : resolved
}

/** One scheme as a document declares it. */
type AuthScheme = {
  headerName?: string
  kind: SourceAuthChoice | 'unknown' | 'unsupported'
  label: string
}

/**
 * The ways a document says a request may be authenticated. Each entry is one
 * requirement, and every scheme inside it is needed at once: OpenAPI reads several
 * schemes in one requirement as AND and several requirements as OR, so
 * "only one of the Security Requirement Objects needs to be satisfied".
 */
type AuthAlternative = AuthScheme[]

const UNKNOWN_SCHEME: AuthScheme = { kind: 'unknown', label: '' }
const NO_AUTH_SCHEME: AuthScheme = { kind: 'none', label: 'no authentication' }

function inspectJsonAuth(document: Record<string, unknown>): SourceDetectedAuth {
  const requirements = Array.isArray(document.security) ? document.security : undefined
  return detectedAuth(jsonAuthAlternatives(document, requirements))
}

function jsonAuthAlternatives(
  document: Record<string, unknown>,
  requirements: unknown[] | undefined,
): AuthAlternative[] {
  if (requirements?.length === 0) return [[NO_AUTH_SCHEME]]

  const components = objectValue(document.components)
  const schemes =
    objectValue(components?.securitySchemes) ?? objectValue(document.securityDefinitions)

  // Without a root `security` block the document does not say how its schemes
  // combine, so each stands on its own.
  if (!requirements) {
    return Object.values(schemes ?? {}).map((scheme) => [authFromScheme(objectValue(scheme))])
  }
  return requirements.map((requirement) => {
    const names = objectKeys(requirement)
    // An empty requirement is how a document says a request may carry nothing, and
    // it sits beside the others rather than replacing them.
    if (names.length === 0) return [NO_AUTH_SCHEME]
    return names.map((name) => authFromScheme(objectValue(schemes?.[name])))
  })
}

function inspectYamlAuth(lines: string[]): SourceDetectedAuth {
  const requirements = yamlSecurityRequirements(lines)
  if (requirements?.length === 0) return detectedAuth([[NO_AUTH_SCHEME]])

  const componentsIndex = findYamlKey(lines, 'components', 0)
  let schemesIndex =
    componentsIndex >= 0 ? findYamlChildKey(lines, componentsIndex, 'securityschemes') : -1
  if (schemesIndex < 0) schemesIndex = findYamlKey(lines, 'securitydefinitions', 0)
  if (schemesIndex < 0) return unknownAuth()

  // Scheme names arrive lowercased from the scanner on both sides of this lookup.
  const schemes = new Map(
    yamlChildIndexes(lines, schemesIndex).map((index) => [
      yamlKey(lines[index]) ?? '',
      yamlMappingFields(lines, index),
    ]),
  )
  if (!requirements) {
    return detectedAuth([...schemes.values()].map((fields) => [authFromScheme(fields)]))
  }
  return detectedAuth(
    requirements.map((names) =>
      names.length === 0
        ? [NO_AUTH_SCHEME]
        : names.map((name) => authFromScheme(schemes.get(name))),
    ),
  )
}

/**
 * Scheme names under a root `security:` block, one entry per requirement. A `- name:`
 * line opens a requirement and every key indented within it joins the same one, which
 * is how a document spells "all of these at once" — Datadog's two API keys, for
 * instance. Returns nothing when there is no such block, or when it is written as a
 * flow sequence this line-based scanner cannot read. Sequence items may be indented
 * beneath `security:` or start at the same indentation, as YAML permits both.
 */
function yamlSecurityRequirements(lines: string[]): string[][] | undefined {
  const index = findYamlKey(lines, 'security', 0)
  if (index < 0) return undefined
  const scalar = yamlScalar(lines[index])
  if (scalar === '[]') return []
  if (scalar) return undefined

  const requirements: string[][] = []
  let itemIndent = -1
  const keyIndent = indentation(lines[index])
  for (let cursor = index + 1; cursor < lines.length; cursor += 1) {
    const line = lines[cursor]
    if (!line.trim()) continue
    const indent = indentation(line)
    const trimmed = line.trim()
    if (indent <= keyIndent && !trimmed.startsWith('-')) break
    if (trimmed.startsWith('-')) {
      if (itemIndent < 0) itemIndent = indent
      // A deeper `-` is a scope under a scheme name, not another requirement.
      if (indent !== itemIndent) continue
      const key = yamlKey(trimmed.slice(1).trim())
      requirements.push(key ? [key] : [])
      continue
    }
    const key = indent > itemIndent ? yamlKey(trimmed) : null
    if (key) requirements[requirements.length - 1]?.push(key)
  }
  return requirements.length > 0 ? requirements : undefined
}

function authFromScheme(scheme: Record<string, unknown> | undefined): AuthScheme {
  if (!scheme) return UNKNOWN_SCHEME
  const type = stringValue(scheme.type).toLowerCase()
  const httpScheme = stringValue(scheme.scheme).toLowerCase()
  const location = stringValue(scheme.in).toLowerCase()
  const name = stringValue(scheme.name)

  if (type === 'http' && httpScheme === 'bearer') {
    return { kind: 'bearer', label: 'a bearer token' }
  }
  if (type === 'oauth2') {
    return { kind: 'bearer', label: 'an OAuth 2.0 bearer token' }
  }
  if (type === 'openidconnect') {
    return { kind: 'bearer', label: 'an OpenID Connect bearer token' }
  }
  if (type === 'apikey' && location === 'header' && name) {
    return { headerName: name, kind: 'header', label: `an API key in the ${name} header` }
  }
  if (type === 'apikey') {
    return {
      kind: 'unsupported',
      label: `a ${location || 'non-header'} API key`,
    }
  }
  if (type === 'http' && httpScheme) {
    return { kind: 'unsupported', label: `HTTP ${httpScheme} authentication` }
  }
  if (type === 'basic') {
    return { kind: 'unsupported', label: 'HTTP basic authentication' }
  }
  return UNKNOWN_SCHEME
}

/** The single answer the wizard needs: what to preselect, and what the document accepts. */
function detectedAuth(declared: AuthAlternative[]): SourceDetectedAuth {
  // A requirement may name a scheme the document never declares. What can be read
  // stands, and an alternative left with nothing is dropped.
  const named = declared.map(namedSchemes).filter((schemes) => schemes.length > 0)
  // An alternative Coral cannot send is worth naming only when it is all the
  // document offers. Beside a usable one it is noise: X's spec, for instance,
  // declares OAuth 1.0a signing next to a plain bearer token.
  const usable = named.filter(isUsableAlternative)
  const alternatives = dedupeAlternatives(usable.length > 0 ? usable : named)
  if (alternatives.length === 0) return unknownAuth()

  // Only here is the document's own phrasing worth showing: there is no method to
  // name instead, and which scheme it is says why.
  if (usable.length === 0) {
    return { kind: 'unsupported', label: authLabel(alternatives) }
  }
  const chosen = preferredAlternative(alternatives)
  return {
    headerNames: headerNames(chosen),
    kind: choiceOf(chosen),
    // Two alternatives answered by the same method read as one: a document offering a
    // choice of API key headers still asks the user for a custom header.
    kinds: [...new Set(alternatives.map(choiceOf))],
  }
}

/** The method a usable alternative is answered with, whatever it combines. */
function choiceOf(alternative: AuthAlternative): SourceAuthChoice {
  return alternative[0].kind as SourceAuthChoice
}

/**
 * The alternative the credentials step preselects. A bearer one goes first: it needs
 * a token and nothing else, where a header alternative also needs its names right.
 */
function preferredAlternative(alternatives: AuthAlternative[]): AuthAlternative {
  return (
    alternatives.find((schemes) => schemes.every((scheme) => scheme.kind === 'bearer')) ??
    alternatives.find((schemes) => choiceOf(schemes) !== 'none') ??
    alternatives[0]
  )
}

// A document is free to declare a dozen schemes, and a sentence naming all of them
// stops being read.
const MAX_LABELLED_ALTERNATIVES = 3

function authLabel(alternatives: AuthAlternative[]): string {
  const listed = alternatives
    .slice(0, MAX_LABELLED_ALTERNATIVES)
    .map((schemes) => schemes.map((scheme) => scheme.label).join(' and '))
    .join(' or ')
  const rest = alternatives.length - MAX_LABELLED_ALTERNATIVES
  return rest > 0 ? `${listed}, or ${rest} more` : listed
}

function namedSchemes(alternative: AuthAlternative): AuthScheme[] {
  return alternative.filter((scheme) => scheme.kind !== 'unknown')
}

function isUsableAlternative(schemes: AuthAlternative): boolean {
  const kinds = new Set(schemes.map((scheme) => scheme.kind))
  return (
    kinds.size === 1 &&
    schemes.every(
      (scheme) => scheme.kind === 'bearer' || scheme.kind === 'header' || scheme.kind === 'none',
    )
  )
}

function headerNames(alternative: AuthAlternative): string[] {
  return [
    ...new Set(alternative.flatMap((scheme) => (scheme.headerName ? [scheme.headerName] : []))),
  ]
}

/** Two alternatives are the same one when they would read the same. */
function dedupeAlternatives(alternatives: AuthAlternative[]): AuthAlternative[] {
  const seen = new Set<string>()
  return alternatives.filter((schemes) => {
    const key = schemes.map((scheme) => scheme.label).join(' and ')
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function unknownAuth(): SourceDetectedAuth {
  return { kind: 'unknown' }
}

function objectKeys(value: unknown): string[] {
  return Object.keys(objectValue(value) ?? {})
}

async function responseTextWithinLimit(response: Response): Promise<string> {
  const declaredLength = Number(response.headers.get('content-length'))
  if (Number.isFinite(declaredLength) && declaredLength > MAX_DESCRIPTOR_BYTES) {
    throw new Error(`The source document is larger than ${MAX_DESCRIPTOR_SIZE_MB} MB`)
  }
  if (!response.body) return ''

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let bytesRead = 0
  let text = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    bytesRead += value.byteLength
    if (bytesRead > MAX_DESCRIPTOR_BYTES) {
      await reader.cancel()
      throw new Error(`The source document is larger than ${MAX_DESCRIPTOR_SIZE_MB} MB`)
    }
    text += decoder.decode(value, { stream: true })
  }
  return text + decoder.decode()
}

function inspectOpenApiYaml(document: string): {
  auth: SourceDetectedAuth
  description: string
  serverUrl: string
  title: string
} | null {
  const lines = document.replace(/^\uFEFF/, '').split(/\r?\n/)
  const hasVersion = lines.some((line) => {
    if (indentation(line) !== 0) return false
    const key = yamlKey(line)
    return key === 'openapi' || key === 'swagger'
  })
  if (!hasVersion) return null

  const infoIndex = lines.findIndex((line) => indentation(line) === 0 && yamlKey(line) === 'info')
  const serverUrl = inspectYamlServerUrl(lines)
  if (infoIndex < 0) return { auth: inspectYamlAuth(lines), description: '', serverUrl, title: '' }
  const infoIndent = lines.slice(infoIndex + 1).find((line) => line.trim() && indentation(line) > 0)
  if (!infoIndent) return { auth: inspectYamlAuth(lines), description: '', serverUrl, title: '' }
  const fieldIndent = indentation(infoIndent)

  let title = ''
  let description = ''
  for (let index = infoIndex + 1; index < lines.length; index += 1) {
    const line = lines[index]
    if (line.trim() && indentation(line) === 0) break
    if (indentation(line) !== fieldIndent) continue
    const key = yamlKey(line)
    if (key === 'title') title = yamlScalar(line)
    if (key === 'description') {
      const scalar = yamlScalar(line)
      if (scalar === '|' || scalar === '>') {
        description = yamlBlock(lines, index + 1, indentation(line), scalar === '>')
      } else {
        description = scalar
      }
    }
  }
  return { auth: inspectYamlAuth(lines), description, serverUrl, title }
}

/**
 * First plain `- url:` entry under a top-level `servers:` block. Unlike the JSON
 * path this does not resolve `{name}` placeholders — the scanner cannot read the
 * nested `variables` mapping of a sequence item — so a templated entry is skipped
 * and the wizard falls back to asking for the base URL.
 */
function inspectYamlServerUrl(lines: string[]): string {
  const serversIndex = findYamlKey(lines, 'servers', 0)
  if (serversIndex < 0) return ''
  for (let index = serversIndex + 1; index < lines.length; index += 1) {
    const line = lines[index]
    if (line.trim() && indentation(line) === 0) break
    if (!/^-?\s*url\s*:/.test(line.trim())) continue
    const url = yamlScalar(line)
    if (url && !url.includes('{')) return url
  }
  return ''
}

function yamlBlock(lines: string[], start: number, parentIndent: number, folded: boolean): string {
  const values: string[] = []
  for (let index = start; index < lines.length; index += 1) {
    const line = lines[index]
    if (line.trim() && indentation(line) <= parentIndent) break
    values.push(line.trim())
  }
  return values.join(folded ? ' ' : '\n').trim()
}

function yamlKey(line: string): string | null {
  const match = line.trim().match(/^([A-Za-z][A-Za-z0-9_-]*)\s*:/)
  return match?.[1]?.toLowerCase() ?? null
}

function yamlScalar(line: string): string {
  const colon = line.indexOf(':')
  if (colon < 0) return ''
  const value = line
    .slice(colon + 1)
    .trim()
    .replace(/\s+#.*$/, '')
  if (
    value.length >= 2 &&
    ((value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'")))
  ) {
    return value.slice(1, -1).replace(/''/g, "'")
  }
  return value
}

function findYamlKey(lines: string[], key: string, indent: number): number {
  return lines.findIndex(
    (line) => indentation(line) === indent && yamlKey(line) === key.toLowerCase(),
  )
}

function findYamlChildKey(lines: string[], parentIndex: number, key: string): number {
  const parentIndent = indentation(lines[parentIndex])
  const childIndent = nextYamlIndent(lines, parentIndex, parentIndent)
  if (childIndent === undefined) return -1
  for (let index = parentIndex + 1; index < lines.length; index += 1) {
    const line = lines[index]
    if (line.trim() && indentation(line) <= parentIndent) break
    if (indentation(line) === childIndent && yamlKey(line) === key.toLowerCase()) return index
  }
  return -1
}

function yamlChildIndexes(lines: string[], parentIndex: number): number[] {
  const parentIndent = indentation(lines[parentIndex])
  const childIndent = nextYamlIndent(lines, parentIndex, parentIndent)
  if (childIndent === undefined) return []
  const indexes: number[] = []
  for (let index = parentIndex + 1; index < lines.length; index += 1) {
    const line = lines[index]
    if (line.trim() && indentation(line) <= parentIndent) break
    if (line.trim() && indentation(line) === childIndent) indexes.push(index)
  }
  return indexes
}

function nextYamlIndent(
  lines: string[],
  parentIndex: number,
  parentIndent: number,
): number | undefined {
  for (let index = parentIndex + 1; index < lines.length; index += 1) {
    const line = lines[index]
    if (!line.trim()) continue
    const indent = indentation(line)
    if (indent <= parentIndent) return undefined
    return indent
  }
  return undefined
}

function yamlMappingFields(lines: string[], mappingIndex: number): Record<string, string> {
  const fields: Record<string, string> = {}
  const mappingIndent = indentation(lines[mappingIndex])
  const fieldIndent = nextYamlIndent(lines, mappingIndex, mappingIndent)
  if (fieldIndent === undefined) return fields
  for (let index = mappingIndex + 1; index < lines.length; index += 1) {
    const line = lines[index]
    if (line.trim() && indentation(line) <= mappingIndent) break
    if (indentation(line) !== fieldIndent) continue
    const key = yamlKey(line)
    if (key) fields[key] = yamlScalar(line)
  }
  return fields
}

function indentation(line: string): number {
  return line.match(/^\s*/)?.[0].length ?? 0
}

function parseJsonObject(document: string): Record<string, unknown> | null {
  try {
    return objectValue(JSON.parse(document)) ?? null
  } catch {
    return null
  }
}

function hasOpenApiVersion(document: Record<string, unknown>): boolean {
  return typeof document.openapi === 'string' || typeof document.swagger === 'string'
}

/** `openapi` is OpenAPI 3's version key; a Swagger 2 document carries `swagger`. */
function isOpenApi3(document: Record<string, unknown>): boolean {
  return typeof document.openapi === 'string'
}

function objectValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

function stringValue(value: unknown): string {
  return typeof value === 'string' ? value.trim() : ''
}

function fallbackTitle(url: URL): string {
  const pathName = url.pathname
    .split('/')
    .filter(Boolean)
    .at(-1)
    ?.replace(/\.[^.]+$/, '')
  if (pathName && !['openapi', 'swagger', 'schema'].includes(pathName.toLowerCase()))
    return pathName
  return url.hostname.replace(/^www\./, '').split('.')[0] || 'source'
}

function sourceName(title: string): string {
  let name = title
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
  if (!name) name = 'source'
  if (!/^[a-z]/.test(name)) name = `source_${name}`
  if (['coral', 'coral_admin', 'public'].includes(name)) name = `${name}_source`
  return name
}

/**
 * OpenAPI treats an absent or empty `servers` block as a single `/` entry, so the
 * origin of the URL we fetched is the document's own answer for where the API
 * lives — right when the spec is served by the API, wrong when it is served by a
 * file host such as `raw.githubusercontent.com`. One request tells the two apart:
 * a file host has no such route, while an API answers or asks for credentials.
 *
 * Only 404/410 and a failed request rule the origin out. Anything else, 401 and
 * 403 included, means the host routes the path, which is what is being checked.
 * The probe reaches only the origin the caller already had us fetch: a path key
 * is document-controlled, and a network-path one such as `//host/data` would
 * otherwise resolve to a host of the document's choosing. It runs inside
 * discovery's own deadline, and every way it can fail leaves the base URL to the
 * user rather than costing the metadata already read from the document.
 */
async function derivedServerUrl(
  documentUrl: URL,
  probePath: string,
  discoverySignal: AbortSignal,
): Promise<string> {
  if (!probePath) return ''
  try {
    const probeUrl = new URL(probePath, documentUrl.origin)
    if (probeUrl.origin !== documentUrl.origin) return ''
    const response = await fetch(probeUrl, {
      signal: AbortSignal.any([discoverySignal, AbortSignal.timeout(5_000)]),
    })
    return response.status === 404 || response.status === 410 ? '' : documentUrl.origin
  } catch {
    return ''
  }
}

/**
 * A base URL is joined with operation paths that already lead with `/`, so a
 * trailing slash would produce `https://host//data`. `new URL().toString()` adds
 * one to every bare origin, which is what a derived server URL always is.
 */
function trimTrailingSlash(url: string): string {
  return url.endsWith('/') ? url.slice(0, -1) : url
}

/** Resolve a relative `servers[].url` such as `/v1` against the document location. */
function absoluteServerUrl(serverUrl: string, fetchUrl: URL): string {
  if (!serverUrl) return ''
  try {
    return trimTrailingSlash(new URL(serverUrl, fetchUrl).toString())
  } catch {
    return ''
  }
}

/**
 * Where the document was actually served from, which a redirect can move to
 * another host entirely. Both a relative `servers[].url` and an origin probe
 * describe the API relative to the document, not to the URL that was typed.
 */
function documentLocation(response: Response, sourceUrl: URL): URL {
  try {
    return new URL(response.url)
  } catch {
    return sourceUrl
  }
}

function discoveryError(url: string, message: string): SourceDiscoveryData {
  return { message, status: 'error', url }
}

function discoveredSource(
  url: string,
  sourceUrl: URL,
  metadata: {
    auth: SourceDetectedAuth
    description: string
    format: SourceDocumentFormat
    inspectionError?: string
    serverUrl: string
    title: string
  },
): SourceDiscoveryData {
  return {
    auth: metadata.auth,
    description: metadata.description,
    format:
      metadata.format === 'unknown' && /\/(?:mcp|sse)\/?$/i.test(sourceUrl.pathname)
        ? 'mcp'
        : metadata.format,
    name: sourceName(metadata.title || fallbackTitle(sourceUrl)),
    serverUrl: metadata.serverUrl,
    status: 'success',
    title: metadata.title,
    url,
    ...(metadata.inspectionError ? { inspectionError: metadata.inspectionError } : {}),
  }
}
