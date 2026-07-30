/**
 * Parsing Slack's method reference pages.
 *
 * Each page is served as markdown at `<page>.md`, and the layout is regular
 * across all 310 of them:
 *
 * ```
 * ## Facts
 * **Description**<one line>
 * **Method Access**
 * ```GET https://slack.com/api/conversations.list```
 * **Scopes**  Bot token: [`channels:read`](…)  User token: …
 * **Rate Limits**[Tier 2: 20+ per minute](…)
 *
 * ## Arguments
 * ### Required arguments
 * **`channel`**`string`Required
 * <description>
 * _Default:_ `…`
 * _Example:_ `…`
 * ```
 *
 * This is the only source for an operation's HTTP verb, argument names, which
 * of them are required, their defaults, and their descriptions. Response shapes
 * come from the recorded samples instead — the pages describe responses only by
 * example.
 */

import type { HttpMethod, Parameter, ScalarType, Scopes } from '../../core/model.ts'

/** Everything one reference page states about its operation. */
export interface DocsFacts {
  /** The method in the casing the request line uses, e.g. `conversations.list`. */
  method: string
  /** Path relative to the server URL, e.g. `/conversations.list`. */
  path: string
  httpMethod: HttpMethod
  description: string
  scopes: Scopes
  /** The tier label Slack publishes, e.g. `Tier 2: 20+ per minute`. */
  rateLimitTier?: string
  parameters: Parameter[]
  /**
   * Every argument the page documents, including ones omitted from
   * {@link parameters}. Comparing against this is what keeps a deliberate
   * omission from reading as a documentation gap.
   */
  documentedArguments: string[]
  warnings: string[]
}

/**
 * `token` is documented as a required argument on every method, but Coral sends
 * credentials as a manifest-configured header. Emitting it as a query parameter
 * would put a required, unfillable argument on every generated relation.
 */
const AUTH_PARAMETER = 'token'

const SOURCE_LINE = /^Source:\s*(\S+)/m
const DESCRIPTION = /^\*\*Description\*\*(.*)$/m
const REQUEST_LINE = /^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+(https?:\/\/\S+)$/m
const RATE_LIMIT = /^\*\*Rate Limits\*\*\[([^\]]+)]/m
const SCOPE_LINK = /\[`([^`]+)`]\(\/reference\/scopes\//g
const ARGUMENT_HEADING = /^\*\*`([^`]+)`\*\*`([^`]+)`(Required|Optional)$/gm
const DEFAULT_VALUE = /^_Default:_\s*`?([^`\n]*)`?$/m
const EXAMPLE_VALUE = /^_Example:_\s*`?([^`\n]*)`?$/m

const SCALAR_TYPES: Record<string, ScalarType> = {
  string: 'string',
  boolean: 'boolean',
  number: 'number',
  integer: 'integer',
}

export class DocsParseError extends Error {}

export function parseDocsPage(markdown: string, serverUrl: string): DocsFacts {
  const request = REQUEST_LINE.exec(markdown)
  if (request === null) {
    throw new DocsParseError('no HTTP request line found under "Method Access"')
  }
  const httpMethod = request[1]?.toLowerCase() as HttpMethod
  const url = request[2] ?? ''
  if (!url.startsWith(serverUrl)) {
    throw new DocsParseError(
      `request URL '${url}' is not under the configured server '${serverUrl}'`,
    )
  }
  const path = url.slice(serverUrl.length)
  const method = path.replace(/^\//, '')

  const warnings: string[] = []
  const { parameters, documentedArguments, warnings: argumentWarnings } = parseArguments(markdown)
  warnings.push(...argumentWarnings)

  const rateLimitTier = RATE_LIMIT.exec(markdown)?.[1]
  return {
    method,
    path,
    httpMethod,
    description: cleanText(DESCRIPTION.exec(markdown)?.[1] ?? ''),
    scopes: parseScopes(markdown),
    ...(rateLimitTier === undefined ? {} : { rateLimitTier }),
    parameters,
    documentedArguments,
    warnings,
  }
}

/** The canonical documentation URL, recorded for provenance. */
export function parseSourceUrl(markdown: string): string | undefined {
  return SOURCE_LINE.exec(markdown)?.[1]
}

/**
 * Bot and user scopes.
 *
 * Both lists live under a single `**Scopes**` heading, separated by `Bot
 * token:` and `User token:` labels, so the section is split on those labels
 * rather than parsed as structure.
 */
function parseScopes(markdown: string): Scopes {
  const section = sliceBetween(markdown, '**Scopes**', '**Content types**')
  if (section === undefined) {
    return { bot: [], user: [] }
  }
  const userIndex = section.indexOf('User token:')
  const botPart = userIndex === -1 ? section : section.slice(0, userIndex)
  const userPart = userIndex === -1 ? '' : section.slice(userIndex)
  return { bot: scopeNames(botPart), user: scopeNames(userPart) }
}

function scopeNames(text: string): string[] {
  const names = new Set<string>()
  for (const match of text.matchAll(SCOPE_LINK)) {
    if (match[1] !== undefined) {
      names.add(match[1])
    }
  }
  return [...names].toSorted((left, right) => left.localeCompare(right))
}

function parseArguments(markdown: string): {
  parameters: Parameter[]
  documentedArguments: string[]
  warnings: string[]
} {
  const section = sliceBetween(markdown, '## Arguments', '\n## ')
  const parameters: Parameter[] = []
  const documentedArguments: string[] = []
  const warnings: string[] = []
  if (section === undefined) {
    return { parameters, documentedArguments, warnings }
  }

  // Requiredness is stated on each argument heading, so the Required/Optional
  // subheadings carry no information the headings do not already have.
  const headings = [...section.matchAll(ARGUMENT_HEADING)]
  for (const [index, heading] of headings.entries()) {
    const name = heading[1]
    const declaredType = heading[2]
    const required = heading[3] === 'Required'
    if (name === undefined || declaredType === undefined) {
      continue
    }
    if (name === AUTH_PARAMETER) {
      continue
    }
    documentedArguments.push(name)

    const start = (heading.index ?? 0) + heading[0].length
    const end = headings[index + 1]?.index ?? section.length
    const body = section.slice(start, end)

    const type = SCALAR_TYPES[declaredType]
    if (type === undefined) {
      // Coral drops non-scalar parameters anyway, so emitting one would only
      // add a column of noise to the descriptor.
      warnings.push(`argument '${name}' has unsupported type '${declaredType}'; omitted`)
      continue
    }

    const defaultValue = coerce(DEFAULT_VALUE.exec(body)?.[1], type)
    const exampleValue = coerce(EXAMPLE_VALUE.exec(body)?.[1], type)
    parameters.push({
      name,
      in: 'query',
      required,
      description: parseArgumentDescription(body),
      schema: { kind: 'scalar', type },
      ...(defaultValue === undefined ? {} : { default: defaultValue }),
      ...(exampleValue === undefined ? {} : { example: exampleValue }),
    })
  }

  return {
    parameters: parameters.toSorted((left, right) => left.name.localeCompare(right.name)),
    documentedArguments: documentedArguments.toSorted((left, right) => left.localeCompare(right)),
    warnings,
  }
}

/**
 * The first prose paragraph after an argument heading.
 *
 * Boolean arguments render a stray `0` line between the description and the
 * `_Default:_` line, so paragraphs are filtered rather than counted.
 */
function parseArgumentDescription(body: string): string {
  for (const paragraph of body.split(/\n\s*\n/)) {
    const text = paragraph.trim()
    if (text === '' || text.startsWith('_Default:_') || text.startsWith('_Example:_')) {
      continue
    }
    if (/^[\d.]+$/.test(text)) {
      continue
    }
    return cleanText(text)
  }
  return ''
}

/**
 * Read a documented literal as the type its argument declares.
 *
 * Slack writes every default as text, so `false` on a boolean and `100` on a
 * number both arrive as strings. Typing them matters: Coral derives a page-size
 * maximum from the declared default, and a string there would be ignored.
 */
function coerce(raw: string | undefined, type: ScalarType): string | number | boolean | undefined {
  const value = raw?.trim()
  if (value === undefined || value === '') {
    return undefined
  }
  if (type === 'boolean') {
    if (value === 'true' || value === 'false') {
      return value === 'true'
    }
    return undefined
  }
  if (type === 'number' || type === 'integer') {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : undefined
  }
  return value
}

/**
 * Flatten documentation prose into one line of plain text.
 *
 * Descriptions land in the SQL catalog, where a relative markdown link is worse
 * than useless — it renders as a path to nowhere — so links keep their label
 * and drop their target.
 */
function cleanText(text: string): string {
  return text
    .replace(/\[([^\]]*)]\([^)]*\)/g, '$1')
    .replace(/\s+/g, ' ')
    .trim()
}

function sliceBetween(text: string, start: string, end: string): string | undefined {
  const from = text.indexOf(start)
  if (from === -1) {
    return undefined
  }
  const rest = text.slice(from + start.length)
  const to = rest.indexOf(end)
  return to === -1 ? rest : rest.slice(0, to)
}
