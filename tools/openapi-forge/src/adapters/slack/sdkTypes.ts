/**
 * Cross-checking arguments against `@slack/web-api`.
 *
 * The SDK's request types are hand-maintained by Slack, composed from mixins
 * such as `TokenOverridable` and `CursorPaginationEnabled`, and each interface
 * carries the documentation URL of the method it describes — which makes them a
 * usable second opinion on what arguments a method really takes.
 *
 * They are a second opinion and nothing more. The reference pages remain the
 * source of truth: they state the HTTP verb, requiredness and defaults, and the
 * SDK states none of those. What this catches is a page that has drifted — an
 * argument the SDK knows about and the page never mentions, or one the page
 * still lists after the SDK dropped it. Disagreements are reported for a human
 * to resolve in the overlay, never applied automatically.
 *
 * Parsed with TypeScript's own parser rather than by pattern-matching the text.
 * An earlier regex attempt silently attached one method's arguments to another
 * method's interface, which is precisely the kind of error a cross-check exists
 * to catch rather than commit.
 */

import ts from 'typescript'

/** A declaration's own property names and the types it composes. */
interface Declaration {
  properties: Set<string>
  bases: string[]
  /** The method this describes, when its leading comment names one. */
  method?: string
}

const DOCS_METHOD = /https:\/\/docs\.slack\.dev\/reference\/methods\/(\S+)/

/**
 * `token` is on every method through `TokenOverridable`, and Coral sends it as
 * a manifest header rather than an argument.
 */
const IGNORED = new Set(['token'])

/**
 * Argument names per method, keyed by the method's documentation slug in
 * lowercase.
 */
export function parseSdkTypes(files: ReadonlyMap<string, string>): Map<string, Set<string>> {
  const declarations = new Map<string, Declaration>()
  for (const [name, text] of files) {
    collectDeclarations(name, text, declarations)
  }

  const byMethod = new Map<string, Set<string>>()
  for (const [name, declaration] of declarations) {
    if (declaration.method === undefined) {
      continue
    }
    const resolved = new Set<string>()
    for (const property of resolveProperties(name, declarations, new Set())) {
      if (!IGNORED.has(property)) {
        resolved.add(property)
      }
    }
    byMethod.set(declaration.method.toLowerCase(), resolved)
  }
  return byMethod
}

function collectDeclarations(fileName: string, text: string, into: Map<string, Declaration>): void {
  const source = ts.createSourceFile(fileName, text, ts.ScriptTarget.ES2023, true)
  for (const statement of source.statements) {
    if (ts.isInterfaceDeclaration(statement)) {
      const properties = memberNames(statement.members)
      const bases: string[] = []
      for (const clause of statement.heritageClauses ?? []) {
        for (const type of clause.types) {
          bases.push(type.expression.getText(source))
          for (const argument of type.typeArguments ?? []) {
            collectFromType(argument, properties, bases, source)
          }
        }
      }
      into.set(statement.name.text, { properties, bases, ...describedMethod(statement, source) })
      continue
    }
    if (ts.isTypeAliasDeclaration(statement)) {
      const properties = new Set<string>()
      const bases: string[] = []
      collectFromType(statement.type, properties, bases, source)
      into.set(statement.name.text, { properties, bases, ...describedMethod(statement, source) })
    }
  }
}

/**
 * Walk a composed type, gathering inline members and referenced type names.
 *
 * Unions model mutually exclusive arguments — `Emails | UserIDs` — and both
 * sides are collected, because either is a name the method accepts.
 */
function collectFromType(
  node: ts.TypeNode,
  properties: Set<string>,
  bases: string[],
  source: ts.SourceFile,
): void {
  if (ts.isTypeLiteralNode(node)) {
    for (const name of memberNames(node.members)) {
      properties.add(name)
    }
    return
  }
  if (ts.isTypeReferenceNode(node)) {
    bases.push(node.typeName.getText(source))
    // Generic wrappers carry the real composition in their type argument:
    // `OptionalArgument<TokenOverridable & CursorPaginationEnabled & {...}>`
    // holds every argument the method takes.
    for (const argument of node.typeArguments ?? []) {
      collectFromType(argument, properties, bases, source)
    }
    return
  }
  if (ts.isIntersectionTypeNode(node) || ts.isUnionTypeNode(node)) {
    for (const member of node.types) {
      collectFromType(member, properties, bases, source)
    }
    return
  }
  if (ts.isParenthesizedTypeNode(node)) {
    collectFromType(node.type, properties, bases, source)
  }
}

function memberNames(members: readonly ts.TypeElement[]): Set<string> {
  const names = new Set<string>()
  for (const member of members) {
    if (ts.isPropertySignature(member) && member.name !== undefined) {
      const name = ts.isIdentifier(member.name)
        ? member.name.text
        : ts.isStringLiteral(member.name)
          ? member.name.text
          : undefined
      if (name !== undefined) {
        names.add(name)
      }
    }
  }
  return names
}

/** Each request interface is preceded by the URL of the page it mirrors. */
function describedMethod(node: ts.Node, source: ts.SourceFile): { method?: string } {
  const ranges = ts.getLeadingCommentRanges(source.getFullText(), node.getFullStart()) ?? []
  for (const range of ranges) {
    const found = DOCS_METHOD.exec(source.getFullText().slice(range.pos, range.end))
    if (found?.[1] !== undefined) {
      return { method: found[1] }
    }
  }
  return {}
}

function resolveProperties(
  name: string,
  declarations: ReadonlyMap<string, Declaration>,
  seen: Set<string>,
): Set<string> {
  if (seen.has(name)) {
    return new Set()
  }
  seen.add(name)
  const declaration = declarations.get(name)
  if (declaration === undefined) {
    return new Set()
  }
  const properties = new Set(declaration.properties)
  for (const base of declaration.bases) {
    for (const inherited of resolveProperties(base, declarations, seen)) {
      properties.add(inherited)
    }
  }
  return properties
}

/**
 * Compare one method's extracted arguments with the SDK's.
 *
 * Returns a warning per disagreement, or none when the two agree or the SDK
 * does not cover the method.
 */
export function crossCheckArguments(
  method: string,
  extracted: readonly string[],
  sdk: ReadonlyMap<string, Set<string>>,
): string[] {
  const known = sdk.get(method.toLowerCase())
  if (known === undefined) {
    return []
  }
  const have = new Set(extracted)
  const missing = [...known].filter((name) => !have.has(name)).toSorted()
  const extra = [...have].filter((name) => !known.has(name)).toSorted()

  const warnings: string[] = []
  if (missing.length > 0) {
    warnings.push(
      `@slack/web-api accepts arguments the reference page does not document: ${missing.join(', ')}`,
    )
  }
  if (extra.length > 0) {
    warnings.push(
      `the reference page documents arguments @slack/web-api does not accept: ${extra.join(', ')}`,
    )
  }
  return warnings
}
