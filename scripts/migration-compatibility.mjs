#!/usr/bin/env node
// Reads migrations and names each statement that can break a Coral release
// that still runs while a newer one migrates the database, during a rolling
// upgrade. scripts/check-migrations.sh runs it on the migrations a checkout
// adds.
//
//   node scripts/migration-compatibility.mjs [--releases "v1.8.0 v1.9.0"] <file>...
//
// A migration whose statements are named declares, in a comment, why an older
// release keeps working:
//
//   -- rolling-upgrade: unused since v1.9.0. <what stopped using it>
//   -- rolling-upgrade: compatible. <why no statement breaks an older release>
//
// One declaration covers the file. `compatible` does not cover a statement
// that removes, renames or retypes a table or a column, which `will` marks.
// A table rebuild is the exception: a table the migration creates takes the
// dropped one's name, and the declaration says that it keeps every column.
//
// It reads SQL text, so it does not see what a statement means: a stored value
// that an older release cannot decode passes. AGENTS.md has the rule for those.
import { readFileSync } from 'node:fs'
import { pathToFileURL } from 'node:url'

const DECLARATION = /^--[ \t]*rolling-upgrade:[ \t]*(.*)$/i
const IDENTIFIER_START = /[A-Za-z0-9_$]/
// A quoted name that names the same object as the bare word.
const PLAIN_NAME = /^[a-z_][a-z0-9_$]*$/

// Splits `sql` into its SQL and its `--` comments. Every string body is
// emptied, and a quoted identifier that is not a plain lowercase name becomes
// `"#N"`, with its text at `quoted[N]`. So a keyword, a quote or a `;` inside
// either is never read as SQL, and two names stay equal only when they are.
function strip(sql) {
  const comments = []
  const quoted = []
  let out = ''
  let i = 0
  while (i < sql.length) {
    const dollar = sql[i] === '$' ? /^\$[A-Za-z_]*\$/.exec(sql.slice(i, i + 64)) : null
    if (sql.startsWith('--', i)) {
      const end = sql.indexOf('\n', i)
      comments.push(sql.slice(i, end === -1 ? sql.length : end).trim())
      i = end === -1 ? sql.length : end
    } else if (sql.startsWith('/*', i)) {
      const end = sql.indexOf('*/', i + 2)
      // A comment separates the words around it, as in `ALTER/**/TABLE`.
      out += ' '
      i = end === -1 ? sql.length : end + 2
    } else if (dollar) {
      const end = sql.indexOf(dollar[0], i + dollar[0].length)
      out += "''"
      i = end === -1 ? sql.length : end + dollar[0].length
    } else if (sql[i] === "'") {
      // Only an `E'…'` string escapes with a backslash.
      const escapes = /e/i.test(sql[i - 1] ?? '') && !IDENTIFIER_START.test(sql[i - 2] ?? '')
      i += 1
      while (i < sql.length) {
        if (escapes && sql[i] === '\\') i += 2
        else if (sql[i] !== "'") i += 1
        else if (sql[i + 1] === "'") i += 2
        else break
      }
      out += "''"
      i += 1
    } else if (sql[i] === '"') {
      let end = i + 1
      while (end < sql.length && (sql[end] !== '"' || sql[end + 1] === '"')) end += sql[end] === '"' ? 2 : 1
      const text = sql.slice(i + 1, end).replaceAll('""', '"')
      if (PLAIN_NAME.test(text)) {
        out += `"${text}"`
      } else {
        const known = quoted.indexOf(text)
        out += `"#${known === -1 ? quoted.push(text) - 1 : known}"`
      }
      i = end + 1
    } else {
      out += sql[i]
      i += 1
    }
  }
  return { sql: out, comments, quoted }
}

// Splits on `separator` outside parentheses.
function splitTopLevel(text, separator) {
  const parts = []
  let depth = 0
  let start = 0
  for (let i = 0; i < text.length; i += 1) {
    if (text[i] === '(') depth += 1
    else if (text[i] === ')') depth -= 1
    else if (depth === 0 && text.startsWith(separator, i)) {
      parts.push(text.slice(start, i))
      start = i + separator.length
      i = start - 1
    }
  }
  parts.push(text.slice(start))
  return parts.map((part) => part.trim()).filter(Boolean)
}

// A name, schema-qualified or not, with each part quoted or bare.
const PART = '(?:"[^"]+"|[a-z_][a-z0-9_$]*)'
const IDENTIFIER = `(${PART}(?:\\.${PART})*)`
const name = (identifier) => identifier.replaceAll('"', '')
const match = (statement, pattern) => new RegExp(pattern).exec(statement)

// One way a statement can break an older release. A `certain` one breaks every
// release that uses what it removes, so no `compatible` declaration covers
// it: the migration names the release that stopped.
const may = (text) => ({ text, certain: false })
const will = (text) => ({ text, certain: true })
const REWRITES = may('rewrites values that an older release reads')

// What one `ALTER TABLE` action can break, or nothing. A column that this
// migration added is no release's yet, so dropping, renaming or retyping it
// breaks none.
function alterAction(action, table, known) {
  if (known.tables.has(table)) return null
  const own = (column) => known.columns.has(`${table}.${name(column)}`)
  if (/^drop\s+constraint\b/.test(action)) return may('drops a constraint')
  // COLUMN is optional in `DROP [COLUMN] name`.
  const dropped = match(action, `^drop\\s+(column\\s+)?(if\\s+exists\\s+)?${IDENTIFIER}`)
  if (dropped && known.columns.delete(`${table}.${name(dropped[3])}`)) return null
  if (/^drop\b/.test(action)) return will('drops a column')
  if (/^rename\s+constraint\b/.test(action)) return may('renames a constraint')
  const renamed = match(action, `^rename\\s+(column\\s+)?${IDENTIFIER}\\s+to\\s+${IDENTIFIER}`)
  if (renamed && known.columns.delete(`${table}.${name(renamed[2])}`)) {
    known.columns.add(`${table}.${name(renamed[3])}`)
    return null
  }
  if (/^rename\b/.test(action)) return will('renames a table or a column')
  if (/^set\s+schema\b/.test(action)) return will('moves a table to another schema')
  const column = `^alter\\s+(column\\s+)?${IDENTIFIER}\\s+`
  const retyped = match(action, `${column}(set\\s+data\\s+)?type\\b`)
  if (retyped) return own(retyped[2]) ? null : will('changes a column type')
  // An older release's insert leaves even a new column without a value.
  if (match(action, `${column}set\\s+not\\s+null\\b`)) {
    return may('requires a value that an older release may not write')
  }
  if (match(action, `${column}drop\\s+default\\b`)) return may('drops a default that an older release may rely on')
  if (/^add\s+(constraint|check|unique|primary\s+key|foreign\s+key|exclude)\b/.test(action)) {
    return may('adds a constraint that an older release may not meet')
  }
  const added = match(action, `^add\\s+(column\\s+)?(if\\s+not\\s+exists\\s+)?${IDENTIFIER}`)
  if (added) {
    // With IF NOT EXISTS the column may be one that a release already reads.
    if (!added[2]) known.columns.add(`${table}.${name(added[3])}`)
    // `DEFAULT NULL` supplies no value.
    if (/\bnot\s+null\b/.test(action) && !/\b(default\s+(?!null\b)|generated\b)/.test(action)) {
      return may('adds a required column that an older release does not write')
    }
  }
  return null
}

// What one lowercase statement can break, as a list of reasons. `known`
// collects the tables and columns this migration adds: no release reads or
// writes those yet, so the migration may change them as it likes.
function statementBreaks(statement, known) {
  const created = match(statement, `^create\\s+(unlogged\\s+)?table\\s+(if\\s+not\\s+exists\\s+)?${IDENTIFIER}`)
  if (created) {
    // With IF NOT EXISTS the table may be one that a release already uses.
    if (!created[2]) known.tables.add(name(created[3]))
    return []
  }
  const droppedTables = match(statement, '^drop\\s+table\\s+(if\\s+exists\\s+)?(.*)$')
  if (droppedTables) {
    const own = splitTopLevel(droppedTables[2], ',').every((target) => {
      const table = match(target, `^${IDENTIFIER}`)
      return table && known.tables.has(name(table[1]))
    })
    if (own) return []
    // A rebuild: a table this migration created takes the dropped one's name.
    const rebuilt = splitTopLevel(droppedTables[2], ',').every((target) => {
      const table = match(target, `^${IDENTIFIER}`)
      return table && known.rebuilt.has(name(table[1]))
    })
    // The check has no record of the dropped table's columns, so it cannot
    // tell whether the new table keeps them: the declaration must say so.
    if (rebuilt) return [may('rebuilds a table')]
  }
  const dropped = match(statement, '^drop\\s+(materialized\\s+view|[a-z]+)')
  if (dropped) {
    const kind = dropped[1].replace(/\s+/g, ' ')
    return [kind === 'table' ? will('drops the table') : may(`drops the ${kind}`)]
  }
  if (/^alter\s+(index|type|view|sequence)\b.*\brename\b/.test(statement)) {
    return [may('renames what an older release names')]
  }

  const altered = match(statement, `^alter\\s+table\\s+(if\\s+exists\\s+)?(only\\s+)?${IDENTIFIER}\\s+(.*)$`)
  if (altered) {
    const table = name(altered[3])
    return splitTopLevel(altered[4], ',')
      .map((action) => alterAction(action, table, known))
      .filter(Boolean)
  }

  const unique = match(statement, `^create\\s+unique\\s+index\\b.*?\\bon\\s+(only\\s+)?${IDENTIFIER}`)
  if (unique && !known.tables.has(name(unique[2]))) {
    return [may('adds a unique index that an older release may not meet')]
  }

  const removed = match(statement, `^(delete\\s+from|truncate(\\s+table)?)\\s+(only\\s+)?${IDENTIFIER}`)
  if (removed && !known.tables.has(name(removed[4]))) return [may('deletes rows that an older release may read')]

  const updated = match(statement, `^update\\s+(only\\s+)?${IDENTIFIER}\\s+(.*?\\s)?set\\s+(.*)$`)
  if (updated && !known.tables.has(name(updated[2]))) {
    const table = name(updated[2])
    const assignments = splitTopLevel(splitTopLevel(splitTopLevel(updated[4], ' where ')[0], ' from ')[0], ',')
    // An update that sets only columns this migration adds is a backfill.
    const backfill = assignments.every((assignment) => {
      const target = match(assignment, `^${IDENTIFIER}\\s*=`)
      return target && known.columns.has(`${table}.${name(target[1])}`)
    })
    if (!backfill) return [REWRITES]
  }

  const trigger = match(statement, `^create\\s+(or\\s+replace\\s+)?(constraint\\s+)?trigger\\b.*?\\bon\\s+${IDENTIFIER}`)
  if (trigger && !known.tables.has(name(trigger[3]))) {
    return [may('adds a trigger to a table that an older release writes')]
  }
  // The body is a string here, so nothing in it was read.
  if (/^do\b/.test(statement)) return [may('runs a block that this check cannot read')]

  // A write that another statement carries: read no further into it.
  const replaced = match(statement, `^(insert\\s+or\\s+replace|replace)\\s+into\\s+${IDENTIFIER}`)
  if (replaced && !known.tables.has(name(replaced[2]))) return [REWRITES]
  if (/^merge\b/.test(statement)) return [REWRITES]
  if (/^with\b/.test(statement) && /\b(update|delete|merge)\b/.test(statement)) return [REWRITES]
  const upserted = match(statement, `^insert\\s+into\\s+${IDENTIFIER}\\b.*\\bdo\\s+update\\b`)
  if (upserted && !known.tables.has(name(upserted[1]))) return [REWRITES]
  return []
}

// Every statement of stripped `sql` that can break an older release, with the
// reason.
function breakingStatements(sql, quoted) {
  const statements = sql.split(';').map((raw) => raw.replace(/\s+/g, ' ').trim())
  const known = { tables: new Set(), columns: new Set(), rebuilt: new Set() }
  // A table that a later statement renames a new table to is rebuilt, not
  // removed, so its drop is not certain to break a release.
  const created = new Set()
  for (const statement of statements) {
    const lower = statement.toLowerCase()
    const table = match(lower, `^create\\s+(unlogged\\s+)?table\\s+${IDENTIFIER}`)
    if (table) created.add(name(table[2]))
    const renamed = match(lower, `^alter\\s+table\\s+${IDENTIFIER}\\s+rename\\s+to\\s+${IDENTIFIER}`)
    if (renamed && created.has(name(renamed[1]))) known.rebuilt.add(name(renamed[2]))
  }
  const found = []
  for (const statement of statements) {
    const shown = statement.replace(/"#(\d+)"/g, (_, index) => `"${quoted[index]}"`)
    const reasons = statementBreaks(statement.toLowerCase(), known)
    if (reasons.length > 0) {
      found.push({
        certain: reasons.some(({ certain }) => certain),
        reason: [...new Set(reasons.map(({ text }) => text))].join(', '),
        statement: shown.length > 100 ? `${shown.slice(0, 97)}...` : shown,
      })
    }
  }
  return found
}

// Why `declaration` does not stand for `breaking`, or nothing.
function declarationProblem(declaration, releases, breaking) {
  const unused = /^unused since (\S+?)\.\s+\S/i.exec(declaration)
  if (unused) {
    return releases.includes(unused[1]) ? null : `names ${unused[1]}, which is not a release this checkout builds on`
  }
  if (/^compatible\.\s+\S/i.test(declaration)) {
    return breaking.some(({ certain }) => certain)
      ? 'cannot be `compatible`: a statement below breaks every release that uses what it removes. ' +
          'Declare `unused since vX.Y.Z. <what stopped using it>` instead'
      : null
  }
  return 'must read `unused since vX.Y.Z. <what stopped using it>` or `compatible. <why>`'
}

// The problems of one migration, as lines to print. Empty when it passes.
export function check(file, text, releases) {
  const { sql, comments, quoted } = strip(text)
  const breaking = breakingStatements(sql, quoted)
  const declarations = comments.flatMap((comment) => {
    const found = DECLARATION.exec(comment)
    return found ? [found[1].trim()] : []
  })
  if (declarations.length > 1) return [`${file} declares rolling-upgrade more than once; keep one declaration`]
  const [declaration] = declarations
  if (breaking.length === 0) {
    return declaration === undefined
      ? []
      : [`${file} declares rolling-upgrade, but no statement in it needs the declaration; remove it`]
  }
  const lines = (statements) => statements.map(({ reason, statement }) => `  ${reason}: ${statement}`)
  if (declaration !== undefined) {
    const problem = declarationProblem(declaration, releases, breaking)
    if (!problem) return []
    return [
      `${file}: its rolling-upgrade declaration ${problem}`,
      ...lines(breaking.filter(({ certain }) => certain && /^compatible/i.test(declaration))),
    ]
  }
  return [`${file} can break a Coral release that still runs during a rolling upgrade:`, ...lines(breaking)]
}

const HELP = `an older Coral release must keep working after a migration runs.
  Keep the step additive: a new table, a new index, a new column that is nullable or has a default.
  To remove or tighten something, stagger it. First ship a release that no longer uses it. Then add
  the migration in a later release, and declare in it:
    -- rolling-upgrade: unused since vX.Y.Z. <what stopped using it>
  If no statement named above breaks an older release, declare:
    -- rolling-upgrade: compatible. <why>
  That covers a constraint, an index or a rewrite of rows. It does not cover a dropped or renamed
  table or column, or a changed column type. A table rebuild is the exception: when a table that
  the migration creates takes the dropped table's name, say that it keeps every column.
  A step with a postgres/ and a sqlite/ half declares in the postgres/ half.`

function main(args) {
  let releases = []
  let files = args
  if (args[0] === '--releases') {
    releases = args[1].split(/\s+/).filter(Boolean)
    files = args.slice(2)
  }
  const problems = files.flatMap((file) => check(file, readFileSync(file, 'utf8'), releases))
  if (problems.length === 0) return 0
  for (const problem of problems) console.error(`check-migrations: ${problem}`)
  console.error(`check-migrations: ${HELP}`)
  return 1
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = main(process.argv.slice(2))
}
