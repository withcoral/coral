/**
 * Parsing Slack's scope reference pages.
 *
 * Every scope has a page at `/reference/scopes/<slug>`, and the method pages
 * link to it. The page opens with a one-line description — exactly what
 * `securitySchemes.*.flows.*.scopes` wants — and states which token types can
 * hold the scope, which independently confirms the bot/user split read off the
 * method pages.
 *
 * ```
 * # channels:read scope
 * View basic information about public channels in a workspace
 * ## Facts
 * **Supported token types**
 * [`Bot`](/authentication/tokens#bot)[`User`](/authentication/tokens#user)
 * ```
 */

const HEADING = /^#\s+(\S+)\s+scope\s*$/m
const TOKEN_TYPE = /\[`([^`]+)`]\(\/authentication\/tokens#/g

/** One scope, as its reference page describes it. */
export interface ScopeFacts {
  /** The scope itself, e.g. `channels:read`. */
  name: string
  description: string
  /** Token classes that may hold it, lowercased: `bot`, `user`, … */
  tokenClasses: string[]
}

export class ScopeParseError extends Error {}

export function parseScopePage(markdown: string): ScopeFacts {
  const heading = HEADING.exec(markdown)
  if (heading?.[1] === undefined) {
    throw new ScopeParseError('no "# <scope> scope" heading found')
  }

  return {
    name: heading[1],
    description: descriptionAfter(markdown, heading.index + heading[0].length),
    tokenClasses: tokenClasses(markdown),
  }
}

/**
 * The first prose line after the heading.
 *
 * Stops at the `## Facts` section, so a page with no description yields an
 * empty string rather than swallowing the section that follows it.
 */
function descriptionAfter(markdown: string, from: number): string {
  for (const line of markdown.slice(from).split('\n')) {
    const text = line.trim()
    if (text === '') {
      continue
    }
    if (text.startsWith('#')) {
      return ''
    }
    return text
  }
  return ''
}

function tokenClasses(markdown: string): string[] {
  const classes = new Set<string>()
  for (const match of markdown.matchAll(TOKEN_TYPE)) {
    if (match[1] !== undefined) {
      classes.add(match[1].toLowerCase())
    }
  }
  return [...classes].toSorted((left, right) => left.localeCompare(right))
}
