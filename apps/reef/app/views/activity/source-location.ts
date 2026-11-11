// Activity shows accepted source snapshots. A highlight is only ever retained
// Coral evidence: a failed Run's top error frame, or the location Coral stored
// for a tool call, emission or log.

import { sourceLines } from '@/wax/components/code-block'
import { privateToolPath, sharedToolPath } from '@/lib/call-root'
import type { ActivityRow, RecordSourceLocation, RunDetail } from './types'

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
const privateToolDocumentId = (privateTool: string) => `private-tool:${privateTool}`
const sharedToolDocumentId = (sharedTool: string) => `shared-tool:${sharedTool}`

const PROGRAM_FRAME_SOURCE = 'Program'
/** A Private Tool's frames name it by its Call Path after this prefix. */
const PRIVATE_TOOL_FRAME_SOURCE_PREFIX = 'Private Tool '
/** A Shared Tool's frames name it by its Call Path after this prefix. */
const SHARED_TOOL_FRAME_SOURCE_PREFIX = 'Shared Tool '

/**
 * The accepted program, then every Private Tool and every Shared Tool Revision
 * the Run linked, as accepted.
 */
export function sourceDocumentsFor(context: RunDetail): SourceDocument[] {
  return [
    {
      id: ACCEPTED_PROGRAM_DOCUMENT_ID,
      code: context.source,
      fileName: `program-${context.run.name.split('/').at(-1)}.ts`,
    },
    ...context.linkedPrivateTools.map((linked) => ({
      id: privateToolDocumentId(linked.privateTool),
      label: `Private tool snapshot: ${linked.path}`,
      code: linked.source,
      fileName: `${linked.path}.ts`,
    })),
    ...context.linkedSharedTools.map((linked) => ({
      id: sharedToolDocumentId(linked.sharedTool),
      label: `Shared tool snapshot: ${linked.path}, revision ${linked.revision}`,
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
 * neither the accepted program nor one of this Run's linked Private or Shared Tools,
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
 * Run's accepted program or one of its linked Private or Shared Tool snapshots. Null
 * for any other record, a record with no stored location, a record of another Run, a snapshot
 * this Run did not link, or a line or column outside the document. A column
 * counts UTF-16 code units and may sit one past the end of its line. A call a
 * tool's body made is never in the program: no callback crosses a tool call, so
 * such a location is not shown as the program's.
 */
export function recordedLocationFor(context: RunDetail, row: ActivityRow): SourceLocation | null {
  if (!madeByCall(row) || row.run !== context.run.name) return null
  const location = row.sourceLocation
  if (!location) return null
  const { document: source } = location
  if (row.kind === 'invocation' && row.parent && source.kind === 'program') return null
  const documentId = documentIdForRecordedSource(source, context)
  if (documentId === null) return null
  const document = sourceDocumentsFor(context).find((candidate) => candidate.id === documentId)!
  if (!shownAt(document.code, location.line, location.column)) return null
  return { documentId, highlight: { startLine: location.line, endLine: location.line } }
}

/** The document a stored location names, when this Run linked it. */
function documentIdForRecordedSource(
  source: RecordSourceLocation['document'],
  context: RunDetail,
): string | null {
  switch (source.kind) {
    case 'program':
      return ACCEPTED_PROGRAM_DOCUMENT_ID
    case 'private_tool':
      return context.linkedPrivateTools.some((linked) => linked.privateTool === source.privateTool)
        ? privateToolDocumentId(source.privateTool)
        : null
    case 'shared_tool':
      return context.linkedSharedTools.some((linked) => linked.sharedTool === source.sharedTool)
        ? sharedToolDocumentId(source.sharedTool)
        : null
    default:
      return null
  }
}

function documentIdForFrameSource(frameSource: string, context: RunDetail): string | null {
  if (frameSource === PROGRAM_FRAME_SOURCE) return ACCEPTED_PROGRAM_DOCUMENT_ID
  if (frameSource.startsWith(SHARED_TOOL_FRAME_SOURCE_PREFIX)) {
    const path = sharedToolPath(frameSource.slice(SHARED_TOOL_FRAME_SOURCE_PREFIX.length))
    const linked = context.linkedSharedTools.find((candidate) => candidate.path === path)
    return linked ? sharedToolDocumentId(linked.sharedTool) : null
  }
  const path = frameSource.startsWith(PRIVATE_TOOL_FRAME_SOURCE_PREFIX)
    ? privateToolPath(frameSource.slice(PRIVATE_TOOL_FRAME_SOURCE_PREFIX.length))
    : null
  if (path === null) return null
  const linked = context.linkedPrivateTools.find((candidate) => candidate.path === path)
  return linked ? privateToolDocumentId(linked.privateTool) : null
}
