import { readFileSync } from 'node:fs'
import path from 'node:path'
import { describe as suite, expect, it } from 'vitest'
import { z } from 'zod'
import type { DiscoveryContext } from '../types'
import { stored } from './fixtures'
import { descriptionSchema, discoveryView, searchPageSchema } from './response'

const search: DiscoveryContext = {
  kind: 'search',
  query: 'github pulls',
  provider: null,
  limit: 5,
  cursor: null,
}

const updateBranch = {
  callPath: 'coral.providers.github.pulls.updateBranch',
  signatureLine:
    'updateBranch(input: { body?: PutReposOwnerRepoPullsPull_numberUpdateBranchBodyInput; owner: string; pull_number: number; repo: string }): Promise<{ data: {…} }>',
  descriptionSnippet:
    'Updates the pull request branch with the latest upstream changes by merging HEAD from the base branch into the pull request branch.\nNote: If making a request on behalf of a GitHub App you must also have permissions to write the contents of the head repository.',
  guidanceSnippet: null,
  tool: { approvalRequired: false, deprecated: false, hints: {} },
  currentHref: null,
}

it('reads a search page into hits, with no coverage when every retriever searched', () => {
  const view = discoveryView(search, stored.complete)
  if (view.kind !== 'search') throw new Error(`expected a search page, got ${view.kind}`)
  expect(view.page.hasMore).toBe(true)
  expect(view.page.coverage).toEqual([])
  expect(view.page.hits).toHaveLength(5)
  expect(view.page.hits[4]).toEqual({
    ...updateBranch,
    kind: 'provider_tool',
    // A hit names no title, so it shows its Call Path until the loader links
    // it to its provider and shortens it.
    title: 'coral.providers.github.pulls.updateBranch',
    matched: { fields: [], tags: ['pulls'], paths: [] },
  })
})

it('reads the kind of each hit from its Call Path, and an absent flag as unset', () => {
  const view = discoveryView(search, stored.mixed)
  if (view.kind !== 'search') throw new Error(`expected a search page, got ${view.kind}`)
  const [savedFunction, merge, subIssues] = view.page.hits
  expect(savedFunction).toMatchObject({
    kind: 'saved_function',
    title: 'github.openIssuesByLabel',
    tool: null,
  })
  expect(merge).toMatchObject({
    kind: 'provider_tool',
    guidanceSnippet: 'API method documentation',
    tool: {
      approvalRequired: true,
      deprecated: false,
      hints: { destructive: true, idempotent: false },
    },
  })
  expect(subIssues.tool).toEqual({ approvalRequired: false, deprecated: false, hints: {} })
  // An argument the signature line could not show reads as its declaration path.
  expect(subIssues.matched.paths).toEqual(['input.per_page'])
})

it('reads Search Coverage for a kind whose results may be missing', () => {
  const view = discoveryView(search, stored.partial)
  if (view.kind !== 'search') throw new Error(`expected a search page, got ${view.kind}`)
  expect(view.page.coverage).toEqual([
    {
      kind: 'provider_tool',
      state: 'partial',
      eligible: 3,
      searched: 2,
      failed: 1,
      returned: 3,
      hasMore: false,
    },
  ])
})

it('reads an untrimmed page with its titles, full coverage, and declaration paths', () => {
  const view = discoveryView(search, stored.untrimmed)
  if (view.kind !== 'search') throw new Error(`expected a search page, got ${view.kind}`)
  expect(view.page.hasMore).toBe(true)
  expect(view.page.coverage).toEqual([
    {
      kind: 'saved_function',
      state: 'empty',
      eligible: 0,
      searched: 0,
      failed: 0,
      returned: 0,
      hasMore: false,
    },
    {
      kind: 'provider_tool',
      state: 'results_found',
      eligible: 1,
      searched: 1,
      failed: 0,
      returned: 100,
      hasMore: true,
    },
  ])
  expect(view.page.hits).toHaveLength(5)
  expect(view.page.hits[4]).toEqual({
    ...updateBranch,
    kind: 'provider_tool',
    title: 'Update a pull request branch',
    matched: {
      fields: ['call_path', 'description', 'provider_display_name', 'provider_namespace', 'tags'],
      tags: ['pulls'],
      paths: ['input.body.expected_head_sha'],
    },
  })
})

it('keeps the titles and paths of an untrimmed page whose coverage the current shape accepts', () => {
  const page = JSON.parse(stored.untrimmed)
  page.retriever_statuses = page.retriever_statuses
    .slice(1)
    .map((status: object) => ({ ...status, state: 'partial' }))
  const view = discoveryView(search, JSON.stringify(page))
  if (view.kind !== 'search') throw new Error(`expected a search page, got ${view.kind}`)
  expect(view.page.hits[4]).toMatchObject({
    title: 'Update a pull request branch',
    matched: { paths: ['input.body.expected_head_sha'] },
  })
})

const exact: DiscoveryContext = {
  kind: 'describe',
  callPath: 'coral.functions.github.openIssuesByLabel',
  types: [],
}

it('reads a stored Saved Function description with its source and diagnostics', () => {
  const view = discoveryView(exact, stored.describeFunction)
  if (view.kind !== 'describe') throw new Error(`expected a description, got ${view.kind}`)
  expect(view.description).toMatchObject({
    kind: 'saved_function',
    callPath: 'coral.functions.github.openIssuesByLabel',
    title: 'github.openIssuesByLabel',
    tool: null,
    savedFunction: {
      diagnostics: [
        {
          category: 'error',
          code: 2304,
          message: "Cannot find name 'coral'.",
          line: 8,
          column: 22,
        },
      ],
      diagnosticsTruncated: false,
    },
    currentHref: null,
  })
  expect(view.description.declaration).toMatch(/^export default function openIssuesByLabel/)
  expect(view.description.savedFunction?.source).toMatch(/^export default async function/)
})

it('reads a Saved Function description returned for types, which has no source', () => {
  const {
    source: _source,
    diagnostics: _d,
    diagnostics_truncated: _t,
    ...typesOnly
  } = JSON.parse(stored.describeFunction)
  const view = discoveryView({ ...exact, types: ['Issue'] }, JSON.stringify(typesOnly))
  if (view.kind !== 'describe') throw new Error(`expected a description, got ${view.kind}`)
  expect(view.description).toMatchObject({ kind: 'saved_function', savedFunction: null })
})

it('reads a stored Tool description with its provider facts', () => {
  const view = discoveryView(exact, stored.describeTool)
  if (view.kind !== 'describe') throw new Error(`expected a description, got ${view.kind}`)
  expect(view.description).toMatchObject({
    kind: 'provider_tool',
    callPath: 'coral.providers.github.issues.create',
    title: 'Create an issue',
    tool: {
      providerLabel: 'GitHub',
      approvalRequired: false,
      deprecated: false,
      tags: ['issues'],
      guidance: 'API method documentation',
    },
    savedFunction: null,
  })
})

it.each([
  ['complete', search, 'search'],
  ['untrimmed', search, 'search'],
  ['mixed', search, 'search'],
  ['partial', search, 'search'],
  ['error', search, 'search'],
  ['describeTool', exact, 'describe'],
  ['describeFunction', exact, 'describe'],
  // The response before Search Hits has other keys: Reef shows it as stored.
  ['earlierShape', search, 'raw'],
  ['earlierShape', exact, 'raw'],
  // A search page is not a description.
  ['complete', exact, 'raw'],
  ['complete', { kind: 'unknown' }, 'raw'],
] as const)('reads the stored %s response for a %o request as %s', (fixture, context, kind) => {
  expect(discoveryView(context, stored[fixture]).kind).toBe(kind)
})

it('shows a response that is not JSON as stored', () => {
  expect(discoveryView(search, '{"hits":').kind).toBe('raw')
})

/*
 * Coral owns the response shape. coral-mcp keeps a snapshot of the output
 * schema that `search` and `describe` advertise, and its own test fails when
 * the shape changes. These tests fail when the parser reads a key, a type, or
 * a set of values that the snapshot does not state.
 */
type Schema = Record<string, any>
type Located = { root: Schema; node: Schema }

/** The alternatives one schema position accepts, with references followed. */
function alternatives({ root, node }: Located): Schema[] {
  if (node.$ref) return alternatives({ root, node: root.$defs[node.$ref.split('/').at(-1)] })
  const union = node.anyOf ?? node.oneOf
  if (union) return union.flatMap((member: Schema) => alternatives({ root, node: member }))
  if (Array.isArray(node.type)) return node.type.map((type: string) => ({ ...node, type }))
  return [node]
}
const values = (schema: Schema): unknown[] | null =>
  schema.enum ? schema.enum.toSorted() : 'const' in schema ? [schema.const] : null
/** A scalar's type and its allowed values. Bounds and formats are not compared. */
const scalar = (schema: Schema) => JSON.stringify([schema.type, values(schema)])

const scalars = (all: Schema[]) =>
  [...new Set(all.filter(({ type }) => type !== 'object' && type !== 'array').map(scalar))]
    .toSorted()
    .join(' | ')
const arrays = (all: Schema[]) => all.filter(({ type }) => type === 'array')
// Object alternatives pair by their `kind`, the key that tells them apart.
const objects = (all: Schema[]) =>
  new Map(
    all
      .filter(({ type }) => type === 'object')
      .map((object) => [String(object.properties.kind?.const), object]),
  )
/** Every difference between what the parser reads and what Coral advertises. */
function drift(reads: Located, advertised: Located, at: string): string[] {
  const mine = alternatives(reads)
  const theirs = alternatives(advertised)
  const problems: string[] = []
  if (scalars(mine) !== scalars(theirs))
    problems.push(`${at}: Reef reads ${scalars(mine)}, Coral advertises ${scalars(theirs)}`)

  if (arrays(mine).length !== arrays(theirs).length)
    problems.push(`${at}: only one side is an array`)
  else if (arrays(mine).length)
    problems.push(
      ...drift(
        { root: reads.root, node: arrays(mine)[0].items },
        { root: advertised.root, node: arrays(theirs)[0].items },
        `${at}[]`,
      ),
    )

  const [mineByKind, theirsByKind] = [objects(mine), objects(theirs)]
  for (const kind of new Set([...mineByKind.keys(), ...theirsByKind.keys()])) {
    const [read, stated] = [mineByKind.get(kind), theirsByKind.get(kind)]
    if (!read || !stated) {
      problems.push(`${at}: only ${read ? 'Reef' : 'Coral'} has the kind ${kind}`)
      continue
    }
    for (const [key, property] of Object.entries<Schema>(read.properties)) {
      if (!stated.properties[key]) {
        problems.push(`${at}.${key}: Coral does not advertise this key`)
        continue
      }
      if (read.required?.includes(key) && !stated.required?.includes(key))
        problems.push(`${at}.${key}: Reef requires a key that Coral can leave out`)
      problems.push(
        ...drift(
          { root: reads.root, node: property },
          { root: advertised.root, node: stated.properties[key] },
          `${at}.${key}`,
        ),
      )
    }
  }
  return problems
}

const tools = {
  search: { schema: searchPageSchema, definition: 'SearchResult' },
  describe: { schema: descriptionSchema, definition: 'Description' },
}
/** The differences for one tool, after `change` edits the advertised schema. */
function differences(tool: keyof typeof tools, change: (definitions: Schema) => void = () => {}) {
  const advertised: Schema = JSON.parse(
    readFileSync(
      path.resolve(
        import.meta.dirname,
        '../../../../../../crates/coral-mcp/tests/output-schema',
        `${tool}.json`,
      ),
      'utf8',
    ),
  )
  change(advertised.$defs)
  const reads = z.toJSONSchema(tools[tool].schema)
  return drift(
    { root: reads, node: reads },
    { root: advertised, node: advertised.$defs[tools[tool].definition] },
    tool,
  )
}

suite('the parser and the advertised output schemas', () => {
  it.each(['search', 'describe'] as const)('agree on every key and type of %s', (tool) => {
    expect(differences(tool)).toEqual([])
  })

  it('differ when Coral removes a key that the parser reads', () => {
    expect(
      differences('search', (definitions) => {
        delete definitions.SearchResult.properties.has_more
      }),
    ).toEqual(['search.has_more: Coral does not advertise this key'])
  })

  it('differ when Coral changes the type of a key', () => {
    expect(
      differences('describe', (definitions) => {
        definitions.Description.oneOf[0].properties.title.type = 'string'
      }),
    ).toEqual([
      'describe.title: Reef reads ["null",null] | ["string",null], Coral advertises ["string",null]',
    ])
  })

  it('differ when Coral adds a coverage state or a kind of description', () => {
    expect(
      differences('search', (definitions) => {
        definitions.RetrieverState.enum.push('stale')
      }),
    ).toEqual([
      expect.stringMatching(
        /^search\.retriever_statuses\[\]\.state: Reef reads .* Coral advertises .*"stale"/,
      ),
    ])
    expect(
      differences('describe', (definitions) => {
        definitions.Description.oneOf.push({
          type: 'object',
          properties: { kind: { type: 'string', const: 'recipe' } },
        })
      }),
    ).toEqual(['describe: only Coral has the kind recipe'])
  })
})
