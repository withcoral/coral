import { expect, it } from 'vitest'
import {
  ACCEPTED_PROGRAM_DOCUMENT_ID,
  recordedLocationFor,
  sourceDocumentsFor,
  sourceLocationFor,
} from './source-location'
import type { ActivityRow, ProgramError, RecordSourceLocation, RunDetail } from './types'

// JavaScript's other line terminators, written by code point so they stay visible here.
const LS = String.fromCharCode(0x2028)
const PS = String.fromCharCode(0x2029)

const run: RunDetail['run'] = {
  agent: '',
  name: 'workspaces/finance/runs/one',
  workspace: 'workspaces/finance',
  state: 'failed',
  startedAt: '2026-09-22T09:00:00Z',
  endedAt: '2026-09-22T09:00:01Z',
  durationNanos: '1',
  requiresAction: false,
  task: '',
  actor: 'you',
}
// Three lines: `program()` at line 1, `return 1` at line 2, the closing brace at line 3.
const acceptedSource = 'export default async function program() {\n  return 1\n}'
function context(overrides: Partial<RunDetail> = {}): RunDetail {
  return {
    kind: 'run',
    run,
    source: acceptedSource,
    linkedSharedTools: [],
    linkedPrivateTools: [],
    terminal: { kind: 'unavailable' },
    ...overrides,
  }
}
function error(frame: { source: string; line: number; column: number }): ProgramError {
  return {
    name: 'TypeError',
    message: 'x',
    frames: [frame],
    toolCode: '',
    toolTraceId: '',
    toolStatus: 0,
    toolBody: '',
    toolBodyTruncated: false,
    cause: null,
    errors: [],
    truncated: false,
    causeOmitted: false,
    omittedErrors: 0,
    circular: false,
  }
}
function failed(frame: { source: string; line: number; column: number }, overrides = {}) {
  return context({
    terminal: {
      kind: 'failed',
      code: 'program_execution_failed',
      message: 'x',
      error: error(frame),
    },
    ...overrides,
  })
}

it('lists the accepted program and every linked Private Tool as its own document', () => {
  const documents = sourceDocumentsFor(
    context({
      linkedSharedTools: [],
      linkedPrivateTools: [
        {
          privateTool: 'private-tools/abc',
          path: 'billing.calculate',
          source: 'export default () => 1',
        },
      ],
    }),
  )
  expect(documents.map((document) => document.id)).toEqual([
    ACCEPTED_PROGRAM_DOCUMENT_ID,
    'private-tool:private-tools/abc',
  ])
  expect(documents[1]!.label).toBe('Private tool snapshot: billing.calculate')
})

it('has no location for a Run that has not failed', () => {
  expect(sourceLocationFor(context({ terminal: { kind: 'unavailable' } }))).toBeNull()
  expect(
    sourceLocationFor(context({ terminal: { kind: 'succeeded', finalValue: null } })),
  ).toBeNull()
})

it('has no location for a failed Run with no retained error frame', () => {
  expect(
    sourceLocationFor(
      context({ terminal: { kind: 'failed', code: 'unknown', message: 'x', error: null } }),
    ),
  ).toBeNull()
})

it('maps the topmost Program frame to the accepted program document', () => {
  expect(sourceLocationFor(failed({ source: 'Program', line: 2, column: 3 }))).toEqual({
    documentId: ACCEPTED_PROGRAM_DOCUMENT_ID,
    highlight: { startLine: 2, endLine: 2 },
  })
})

// Catches: a failure inside a Shared Tool shown with no highlighted line, or a
// Shared Tool snapshot missing from the documents the source pane offers.
it('maps a Shared Tool frame to the linked revision, and lists it as its own document', () => {
  const linked = {
    linkedSharedTools: [
      {
        sharedTool: 'sharedTools/abc',
        path: 'mail.remind',
        revision: '3',
        source: 'export default () => 1',
      },
    ],
  }
  expect(
    sourceLocationFor(
      failed({ source: 'Shared Tool coral.shared.mail.remind', line: 1, column: 8 }, linked),
    ),
  ).toEqual({
    documentId: 'shared-tool:sharedTools/abc',
    highlight: { startLine: 1, endLine: 1 },
  })
  expect(sourceDocumentsFor(context(linked)).at(-1)).toMatchObject({
    id: 'shared-tool:sharedTools/abc',
    label: 'Shared tool snapshot: mail.remind, revision 3',
  })
  expect(
    sourceLocationFor(
      failed({ source: 'Shared Tool coral.shared.mail.other', line: 1, column: 8 }, linked),
    ),
  ).toBeNull()
})

// A retained Run keeps the frame sources it recorded under the old root.
it.each(['coral.private', 'coral.functions'])(
  'maps a Private Tool frame under %s to that linked private tool, by the path the Program called it with',
  (root) => {
    const location = sourceLocationFor(
      failed(
        { source: `Private Tool ${root}.billing.calculate`, line: 1, column: 8 },
        {
          linkedSharedTools: [],
          linkedPrivateTools: [
            {
              privateTool: 'private-tools/abc',
              path: 'billing.calculate',
              source: 'export default () => 1',
            },
          ],
        },
      ),
    )
    expect(location).toEqual({
      documentId: 'private-tool:private-tools/abc',
      highlight: { startLine: 1, endLine: 1 },
    })
  },
)

it('never guesses a document for an unrecognized frame source', () => {
  // Names a linked private tool's prefix, but no linked private tool has this path: Coral
  // retained no matching source, so this must not fall back to the accepted program.
  const location = sourceLocationFor(
    failed(
      { source: 'Private Tool coral.private.billing.calculate', line: 1, column: 1 },
      { linkedPrivateTools: [] },
    ),
  )
  expect(location).toBeNull()
})

// A malformed or out-of-range frame must never produce a fabricated highlight.
it.each<[string, { source: string; line: number; column: number }]>([
  ['a zero line', { source: 'Program', line: 0, column: 1 }],
  ['a negative line', { source: 'Program', line: -1, column: 1 }],
  ['a fractional line', { source: 'Program', line: 1.5, column: 1 }],
  ['a NaN line', { source: 'Program', line: NaN, column: 1 }],
  ['an infinite line', { source: 'Program', line: Infinity, column: 1 }],
  // The accepted program is 3 lines; line 4 does not exist in it.
  ['a line past the end of the document', { source: 'Program', line: 4, column: 1 }],
  ['a zero column', { source: 'Program', line: 1, column: 0 }],
  ['a fractional column', { source: 'Program', line: 1, column: 1.5 }],
  // Line 2 is `  return 1`: 10 UTF-16 units, so column 11 is the last valid one.
  ['a column past the end of its line', { source: 'Program', line: 2, column: 12 }],
])('rejects a frame with %s rather than claim a location', (_, frame) => {
  expect(sourceLocationFor(failed(frame))).toBeNull()
})

it('accepts a frame on the document’s exact last line', () => {
  expect(sourceLocationFor(failed({ source: 'Program', line: 3, column: 1 }))).toEqual({
    documentId: ACCEPTED_PROGRAM_DOCUMENT_ID,
    highlight: { startLine: 3, endLine: 3 },
  })
})

// A record's location is the one Coral stored when the call ran. Nothing else picks a line.
const invocationRow: Extract<ActivityRow, { kind: 'invocation' }> = {
  kind: 'invocation',
  name: 'workspaces/finance/invocations/one',
  workspace: 'workspaces/finance',
  actor: 'you',
  agent: '',
  task: '',
  run: run.name,
  provider: 'workspaces/finance/providers/mail',
  tool: 'send',
  callPath: 'mail.send',
  state: 'ended',
  outcome: 'upstream_outcome',
  startedAt: run.startedAt,
  endedAt: run.startedAt,
  durationNanos: '1',
  requiresAction: false,
  dispatchedAt: run.startedAt,
  waitingNanos: '0',
  toolKind: 'provider',
}
const logRow: Extract<ActivityRow, { kind: 'output' }> = {
  kind: 'output',
  name: `${run.name}/events/1`,
  workspace: 'workspaces/finance',
  run: run.name,
  label: 'log #1',
  startedAt: run.startedAt,
  eventKind: 'program.logged',
  agent: '',
}
const privateTool = 'workspaces/finance/privateTools/notify-v2'
// Line 2 holds the only call; 'é' and the astral '😀' make UTF-16 length differ from characters.
const program = [
  'for (const id of [1, 2]) {',
  '  await coral.providers.mail.send({ id }) // é😀',
  '}',
].join('\n')
const sharedTool = 'workspaces/finance/sharedTools/remind'
const retained = context({
  source: program,
  linkedSharedTools: [
    {
      sharedTool,
      path: 'mail.remind',
      revision: '2',
      source: 'export default async () => {\n  console.log("reminded")\n}',
    },
  ],
  linkedPrivateTools: [
    {
      privateTool,
      path: 'billing.notify',
      source: 'export default async () => {\n  console.log("sent")\n}',
    },
  ],
})
const line2Length = program.split('\n')[1]!.length
const at = (
  document: RecordSourceLocation['document'],
  line: number,
  column: number,
): RecordSourceLocation => ({ document, line, column })

it.each<[string, ActivityRow, ReturnType<typeof recordedLocationFor>]>([
  [
    'a tool located in the program',
    { ...invocationRow, sourceLocation: at({ kind: 'program' }, 2, 31) },
    { documentId: ACCEPTED_PROGRAM_DOCUMENT_ID, highlight: { startLine: 2, endLine: 2 } },
  ],
  [
    'a log located in the Private Tool snapshot it names',
    { ...logRow, sourceLocation: at({ kind: 'private_tool', privateTool }, 2, 11) },
    { documentId: `private-tool:${privateTool}`, highlight: { startLine: 2, endLine: 2 } },
  ],
  // Catches: a call a Shared Tool's body made shown with no location, or in the caller's program.
  [
    'a log located in the Shared Tool snapshot it names',
    { ...logRow, sourceLocation: at({ kind: 'shared_tool', sharedTool }, 2, 11) },
    { documentId: `shared-tool:${sharedTool}`, highlight: { startLine: 2, endLine: 2 } },
  ],
  [
    'a Shared Tool snapshot this Run did not link',
    {
      ...logRow,
      sourceLocation: at(
        { kind: 'shared_tool', sharedTool: 'workspaces/finance/sharedTools/other' },
        2,
        11,
      ),
    },
    null,
  ],
  [
    'a column one past the end of its line, counted in UTF-16 units',
    { ...invocationRow, sourceLocation: at({ kind: 'program' }, 2, line2Length + 1) },
    { documentId: ACCEPTED_PROGRAM_DOCUMENT_ID, highlight: { startLine: 2, endLine: 2 } },
  ],
  // Catches: a record without stored metadata falling back to a text match of its call.
  ['a record Coral stored no location for', invocationRow, null],
  [
    'a snapshot this Run did not link, even at the same path',
    {
      ...logRow,
      sourceLocation: at(
        { kind: 'private_tool', privateTool: 'workspaces/finance/privateTools/notify-v1' },
        2,
        11,
      ),
    },
    null,
  ],
  [
    'a record from another Run',
    {
      ...invocationRow,
      run: 'workspaces/finance/runs/other',
      sourceLocation: at({ kind: 'program' }, 2, 3),
    },
    null,
  ],
  // Catches: a Run event that is no call, such as its end, highlighting a line of the program.
  [
    'a Run event that is not a log or an emission',
    { ...logRow, eventKind: 'run.ended', sourceLocation: at({ kind: 'program' }, 2, 31) },
    null,
  ],
  // Catches: a call a tool's body made shown as a line of the program that called the tool.
  [
    'a call a tool made, at a place in the program',
    {
      ...invocationRow,
      parent: {
        invocation: 'workspaces/finance/invocations/notify',
        callPath: 'coral.private.billing.notify',
        toolKind: 'private',
      },
      sourceLocation: at({ kind: 'program' }, 2, 31),
    },
    null,
  ],
  [
    'a call a tool made, in that tool’s snapshot',
    {
      ...invocationRow,
      parent: {
        invocation: 'workspaces/finance/invocations/notify',
        callPath: 'coral.private.billing.notify',
        toolKind: 'private',
      },
      sourceLocation: at({ kind: 'private_tool', privateTool }, 2, 11),
    },
    { documentId: `private-tool:${privateTool}`, highlight: { startLine: 2, endLine: 2 } },
  ],
  ['a zero line', { ...invocationRow, sourceLocation: at({ kind: 'program' }, 0, 1) }, null],
  [
    'a line past the end',
    { ...invocationRow, sourceLocation: at({ kind: 'program' }, 4, 1) },
    null,
  ],
  [
    'a fractional line',
    { ...invocationRow, sourceLocation: at({ kind: 'program' }, 1.5, 1) },
    null,
  ],
  ['a zero column', { ...invocationRow, sourceLocation: at({ kind: 'program' }, 2, 0) }, null],
  [
    'a column two past the end of its line',
    { ...invocationRow, sourceLocation: at({ kind: 'program' }, 2, line2Length + 2) },
    null,
  ],
])('locates %s', (_, row, expected) => {
  expect(recordedLocationFor(retained, row)).toEqual(expected)
})

// Coral counts lines as JavaScript does, and so does the source pane: CRLF ends one line;
// a lone CR, LF, U+2028 and U+2029 each end one. A column counts no line terminator.
// Catches: a stored line highlighting a different line than the one the call is on.
it.each<[string, string, number, number, number | null]>([
  ['an LF line', 'a()\nb()\nc()', 3, 1, 3],
  ['a CRLF line', 'a()\r\nb()\r\nc()', 3, 1, 3],
  ['a column one past a CRLF line, without its CR', 'a()\r\nb()\r\nc()', 2, 4, 2],
  ['a column on the CR of a CRLF line', 'a()\r\nb()\r\nc()', 2, 5, null],
  ['a line ended by a lone CR', 'a()\rb()\nc()', 2, 1, 2],
  ['a line ended by U+2028', `a()${LS}b()\nc()`, 2, 1, 2],
  ['a line ended by U+2029', `a()${PS}b()\nc()`, 2, 1, 2],
])('highlights %s where JavaScript counts it', (_, source, line, column, expected) => {
  const located = recordedLocationFor(context({ source }), {
    ...invocationRow,
    sourceLocation: at({ kind: 'program' }, line, column),
  })
  expect(located?.highlight.startLine ?? null).toBe(expected)
  // A failed Run's frame keeps the same rule.
  const frame = sourceLocationFor(failed({ source: 'Program', line, column }, { source }))
  expect(frame?.highlight.startLine ?? null).toBe(expected)
})

// Catches: an ordinal or sibling order picking the line instead of the record's own location.
it('locates each repeated call at its own stored line, whatever order the records load in', () => {
  const source = 'await coral.providers.mail.send({})\nawait coral.providers.mail.send({})'
  const first = { ...invocationRow, name: 'first', sourceLocation: at({ kind: 'program' }, 2, 29) }
  const second = {
    ...invocationRow,
    name: 'second',
    sourceLocation: at({ kind: 'program' }, 1, 29),
  }
  expect(recordedLocationFor(context({ source }), second)?.highlight.startLine).toBe(1)
  expect(recordedLocationFor(context({ source }), first)?.highlight.startLine).toBe(2)
})
