import { describe, expect, it } from 'vitest'

import { sourceLines } from '@/wax/components/code-block'

import { diffLines, foldUnchanged, shownLines, type DiffLine } from './diff-lines'

/** A diff as one string per line: its marker, both line numbers, and its text. */
const rows = (lines: DiffLine[]) =>
  lines.map(
    ({ kind, before, after, text }) =>
      `${{ same: ' ', removed: '-', added: '+' }[kind]} ${before ?? '.'} ${after ?? '.'} ${text}`,
  )

// Catches: a diff that marks an unchanged line, that numbers a line by its
// place in the diff, not in its own text, or that puts an added line before
// the line it replaces.
it('marks only the lines that differ, each with its number on its own side', () => {
  const before = 'const a = 1\nconst b = 2\nreturn a + b'
  const after = 'const a = 1\nconst b = 3\nconst c = 4\nreturn a + b'
  expect(rows(diffLines(before, after))).toEqual([
    '  1 1 const a = 1',
    '- 2 . const b = 2',
    '+ . 2 const b = 3',
    '+ . 3 const c = 4',
    '  3 4 return a + b',
  ])
})

const big = (lines: number, salt: string) =>
  Array.from({ length: lines }, (_, index) => `${salt} line ${index}`).join('\n')

describe('the two texts a diff must give back', () => {
  // Catches: a diff that drops, repeats or reorders a line of either text,
  // including the texts too different to compare line by line.
  it.each([
    { name: 'the same text', before: 'a\nb\nc', after: 'a\nb\nc' },
    { name: 'nothing before', before: '', after: 'a\nb' },
    { name: 'nothing after', before: 'a\nb', after: '' },
    { name: 'a line added first', before: 'b\nc', after: 'a\nb\nc' },
    { name: 'a line removed last', before: 'a\nb\nc', after: 'a\nb' },
    { name: 'lines that moved', before: 'a\nb\nc\nd', after: 'c\nd\na\nb' },
    { name: 'a repeated line', before: 'x\nx\ny\nx', after: 'x\ny\nx\nx\nx' },
    { name: 'a final line break that is new', before: 'a', after: 'a\n' },
    { name: 'no line in common', before: 'a\nb', after: 'c\nd\ne' },
    {
      name: 'two long texts with no line in common',
      before: big(3000, 'old'),
      after: big(3000, 'new'),
    },
    {
      name: 'a long text with one changed line',
      before: big(5000, 'same'),
      after: big(5000, 'same').replace('same line 2500', 'changed'),
    },
  ])('holds every line of each side in order for $name', ({ before, after }) => {
    const lines = diffLines(before, after)
    const side = (kind: 'removed' | 'added') =>
      lines.filter((line) => line.kind === 'same' || line.kind === kind).map((line) => line.text)
    expect(side('removed')).toEqual(sourceLines(before))
    expect(side('added')).toEqual(sourceLines(after))
    // Each side numbers its own lines from 1 with no gap.
    const numbers = (key: 'before' | 'after') =>
      lines.flatMap((line) => (line[key] === null ? [] : [line[key]]))
    expect(numbers('before')).toEqual(sourceLines(before).map((_, index) => index + 1))
    expect(numbers('after')).toEqual(sourceLines(after).map((_, index) => index + 1))
  })
})

// Catches: an empty unchanged row after the last line of two texts that both
// end with a line break, or a line break that only one text ends with, or an
// empty line inside the text, dropped from the diff.
it.each([
  {
    name: 'both end with a line break',
    before: 'a\nb\n',
    after: 'a\nc\n',
    rows: ['  1 1 a', '- 2 . b', '+ . 2 c'],
  },
  {
    name: 'only the new text ends with one',
    before: 'a',
    after: 'a\n',
    rows: ['  1 1 a', '+ . 2 '],
  },
  {
    name: 'an empty line inside',
    before: 'a\n\nb',
    after: 'a\n\nc',
    rows: ['  1 1 a', '  2 2 ', '- 3 . b', '+ . 3 c'],
  },
])('shows only the lines of the texts when $name', ({ before, after, rows: expected }) => {
  expect(rows(shownLines(diffLines(before, after)))).toEqual(expected)
})

// Catches: a one-line change reported as the whole text removed and added.
it('keeps the unchanged lines of a long text unchanged', () => {
  const before = Array.from({ length: 5000 }, (_, index) => `line ${index}`)
  const after = before.with(2500, 'changed')
  const changed = diffLines(before.join('\n'), after.join('\n')).filter(
    (line) => line.kind !== 'same',
  )
  expect(rows(changed)).toEqual(['- 2501 . line 2500', '+ . 2501 changed'])
})

// Catches: a fold that hides a changed line or its context, that folds a run
// too short to be worth it, or that miscounts the lines it hides.
it('folds each long run of unchanged lines down to the context around a change', () => {
  const before = Array.from({ length: 20 }, (_, index) => `line ${index + 1}`)
  const after = before.with(9, 'changed')
  const folded = foldUnchanged(diffLines(before.join('\n'), after.join('\n')), 2)
  expect(
    folded.map((row) => ('hidden' in row ? `… ${row.hidden} unchanged` : rows([row])[0])),
  ).toEqual([
    '… 7 unchanged',
    '  8 8 line 8',
    '  9 9 line 9',
    '- 10 . line 10',
    '+ . 10 changed',
    '  11 11 line 11',
    '  12 12 line 12',
    '… 8 unchanged',
  ])
  // A run no longer than the context on both sides, plus the fold's own row, stays open.
  const close = before.with(9, 'changed').with(15, 'changed too')
  const open = foldUnchanged(diffLines(before.join('\n'), close.join('\n')), 2)
  expect(open.slice(3, 12).some((row) => 'hidden' in row)).toBe(false)
})
