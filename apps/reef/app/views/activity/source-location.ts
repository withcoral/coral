// Activity shows accepted source snapshots. A highlight is only ever retained
// Coral evidence: a failed Run's top error frame, or the location Coral stored
// for a tool call, emission or log.

import { sourceLines } from '@/wax/components/code-block'
import type { ActivityRow, RunDetail } from './types'

export interface SourceDocument {
  id: string
  /** What the pane names the document; the accepted program, the pane's default, has none. */
  label?: string
  code: string
  /** What a download of `code` is named. */
  fileName: string
}
export interface SourceHighlight {
  startLine: number
  endLine: number
}
export interface SourceLocation {
  documentId: string
  highlight: SourceHighlight
}

export const ACCEPTED_PROGRAM_DOCUMENT_ID = 'accepted-program'
const savedFunctionDocumentId = (savedFunction: string) => `saved-function:${savedFunction}`

const PROGRAM_FRAME_SOURCE = 'Program'
const SAVED_FUNCTION_FRAME_SOURCE_PREFIX = 'Saved Function coral.functions.'

/** The accepted program, then every Saved Function the Run linked, as accepted. */
export function sourceDocumentsFor(context: RunDetail): SourceDocument[] {
  return [
    {
      id: ACCEPTED_PROGRAM_DOCUMENT_ID,
      code: context.source,
      fileName: `program-${context.run.name.split('/').at(-1)}.ts`,
    },
    ...context.linkedFunctions.map((linked) => ({
      id: savedFunctionDocumentId(linked.savedFunction),
      label: `Saved-function snapshot: ${linked.path}`,
      code: linked.source,
      fileName: `${linked.path}.ts`,
    })),
  ]
}

const isPositiveInteger = (value: number) => Number.isInteger(value) && value > 0

/**
 * Whether `line` and `column` (1-based; the column in UTF-16 units, at most one past
 * the line's end) name a place in `code`. Lines are counted as JavaScript counts them,
 * as Coral stores them and as the source pane numbers them.
 */
function shownAt(code: string, line: number, column: number) {
  if (!isPositiveInteger(line) || !isPositiveInteger(column)) return false
  const text = sourceLines(code)[line - 1]
  return text !== undefined && column <= text.length + 1
}

/**
 * Where a failed Run's Program error was thrown. Null for any Run that has
 * not failed, a failed Run with no retained error, a frame whose source names
 * neither the accepted program nor one of this Run's linked Saved Functions,
 * or a frame whose line and column are not a place the source pane shows
 * (see `shownAt`). Coral's retained evidence is always
 * well-formed; these checks exist so a malformed or stale frame fails honest
 * rather than highlighting the wrong line.
 */
export function sourceLocationFor(context: RunDetail): SourceLocation | null {
  if (context.terminal.kind !== 'failed') return null
  const frame = context.terminal.error?.frames[0]
  if (!frame) return null
  const documentId = documentIdForFrameSource(frame.source, context)
  if (documentId === null) return null
  const document = sourceDocumentsFor(context).find((candidate) => candidate.id === documentId)!
  if (!shownAt(document.code, frame.line, frame.column)) return null
  return { documentId, highlight: { startLine: frame.line, endLine: frame.line } }
}

/** A record one call in the author's code made: a tool call, a log or an emission. */
export function madeByCall(
  row: ActivityRow,
): row is Extract<ActivityRow, { kind: 'invocation' | 'output' }> {
  return (
    row.kind === 'invocation' ||
    (row.kind === 'output' &&
      (row.eventKind === 'program.logged' || row.eventKind === 'program.emitted'))
  )
}

/**
 * The location Coral stored for a tool call, emission or log, in this
 * Run's accepted program or one of its linked Saved Function snapshots. Null
 * for any other record, a record with no stored location, a record of another Run, a snapshot
 * this Run did not link, or a line or column outside the document. A column
 * counts UTF-16 code units and may sit one past the end of its line.
 */
export function recordedLocationFor(context: RunDetail, row: ActivityRow): SourceLocation | null {
  if (!madeByCall(row) || row.run !== context.run.name) return null
  const location = row.sourceLocation
  if (!location) return null
  const { document: source } = location
  const documentId =
    source.kind === 'program'
      ? ACCEPTED_PROGRAM_DOCUMENT_ID
      : context.linkedFunctions.some((linked) => linked.savedFunction === source.savedFunction)
        ? savedFunctionDocumentId(source.savedFunction)
        : null
  if (documentId === null) return null
  const document = sourceDocumentsFor(context).find((candidate) => candidate.id === documentId)!
  if (!shownAt(document.code, location.line, location.column)) return null
  return { documentId, highlight: { startLine: location.line, endLine: location.line } }
}

function documentIdForFrameSource(frameSource: string, context: RunDetail): string | null {
  if (frameSource === PROGRAM_FRAME_SOURCE) return ACCEPTED_PROGRAM_DOCUMENT_ID
  const path = frameSource.startsWith(SAVED_FUNCTION_FRAME_SOURCE_PREFIX)
    ? frameSource.slice(SAVED_FUNCTION_FRAME_SOURCE_PREFIX.length)
    : null
  if (path === null) return null
  const linked = context.linkedFunctions.find((candidate) => candidate.path === path)
  return linked ? savedFunctionDocumentId(linked.savedFunction) : null
}
