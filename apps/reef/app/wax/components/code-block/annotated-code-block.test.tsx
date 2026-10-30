/** @vitest-environment happy-dom */
import { render } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { AnnotatedCodeBlock } from './annotated-code-block'

// JavaScript's other line terminators, written by code point so they stay visible here.
const LS = String.fromCharCode(0x2028)
const PS = String.fromCharCode(0x2029)

const code = ['function program() {', '  return 1', '}'].join('\n')

afterEach(() => {
  vi.restoreAllMocks()
})

it('numbers every line and marks only the lines inside the highlighted range', () => {
  const { container } = render(
    <AnnotatedCodeBlock
      code={code}
      language="typescript"
      highlight={{ startLine: 2, endLine: 2 }}
      followHighlight={false}
      revealKey="a"
    />,
  )
  const line = (number: number) => container.querySelector(`[data-line="${number}"]`)!
  expect(line(1)).not.toHaveAttribute('data-highlighted')
  expect(line(2)).toHaveAttribute('data-highlighted', 'true')
  expect(line(3)).not.toHaveAttribute('data-highlighted')
})

it('marks no line when there is no highlight', () => {
  const { container } = render(
    <AnnotatedCodeBlock
      code={code}
      language="typescript"
      highlight={null}
      followHighlight={false}
      revealKey="a"
    />,
  )
  expect(container.querySelectorAll('[data-highlighted]')).toHaveLength(0)
})

const rendered = (source: string) => {
  const { container } = render(
    <AnnotatedCodeBlock
      code={source}
      language="typescript"
      highlight={null}
      followHighlight={false}
      revealKey="a"
    />,
  )
  return [...container.querySelectorAll<HTMLElement>('[data-line]')]
}
/** A line's first coloured token. */
const token = (line: HTMLElement) => line.lastElementChild!.querySelector('span')
/** A line's text, without its line number. */
const lineText = (line: HTMLElement) => line.lastElementChild!.textContent

// Catches: a line numbered differently from JavaScript, so a stored location lands on the
// wrong row. CRLF ends one line; a lone CR, LF, U+2028 and U+2029 each end one.
it('numbers lines as JavaScript counts them', () => {
  const lines = rendered(`a()\r\nb()\rc()\nd()${LS}e()${PS}f()`)
  expect(lines.map((line) => line.dataset.line)).toEqual(['1', '2', '3', '4', '5', '6'])
  expect(lines.map(lineText)).toEqual(['a()', 'b()', 'c()', 'd()', 'e()', 'f()'])
})

// Catches: a split dropping the syntax colour of a token that spans a line break.
it('keeps a token’s colour on both sides of a line break inside it', () => {
  const [first, second] = rendered('/* one\rtwo */')
  expect(token(first!)).toHaveTextContent('/* one')
  expect(token(second!)).toHaveTextContent('two */')
  expect(token(second!)!.className).toBe(token(first!)!.className)
  expect(token(first!)!.className).not.toBe('')
})

// Catches: a source too long to colour rendering as one numbered row.
it('numbers every line of a source too long to colour', () => {
  const lines = rendered(
    Array.from({ length: 2100 }, (_, i) => `// ${i} ${'x'.repeat(100)}`).join('\n'),
  )
  expect(lines).toHaveLength(2100)
  expect(lineText(lines[2099]!)).toContain('// 2099 ')
})

/**
 * `getBoundingClientRect` reports layout happy-dom never computes, so these
 * stub it: a `[data-line]` element gets its own entry; anything else (the
 * scroll viewport itself) gets `rects.viewport`.
 */
function stubRects(rects: Record<string, DOMRect>) {
  vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
    const line = this.getAttribute('data-line')
    const key = line ? `line:${line}` : 'viewport'
    return rects[key] ?? ({ top: 0, bottom: 0, left: 0, right: 0 } as DOMRect)
  })
}
const rect = (top: number, bottom: number) => ({ top, bottom, left: 0, right: 0 }) as DOMRect

it('reveals an out-of-view highlighted range when following selection, and only once per reveal key', () => {
  stubRects({
    viewport: rect(0, 100),
    'line:2': rect(140, 160),
  })
  const scrollTo = vi.fn()
  Element.prototype.scrollTo = scrollTo
  const { rerender } = render(
    <AnnotatedCodeBlock
      code={code}
      language="typescript"
      highlight={{ startLine: 2, endLine: 2 }}
      followHighlight
      revealKey="a"
    />,
  )
  expect(scrollTo).toHaveBeenCalledTimes(1)
  rerender(
    <AnnotatedCodeBlock
      code={code}
      language="typescript"
      highlight={{ startLine: 2, endLine: 2 }}
      followHighlight
      revealKey="a"
    />,
  )
  expect(scrollTo).toHaveBeenCalledTimes(1)
})

it('never scrolls when not following selection, even if the range is out of view', () => {
  stubRects({
    viewport: rect(0, 100),
    'line:2': rect(140, 160),
  })
  const scrollTo = vi.fn()
  Element.prototype.scrollTo = scrollTo
  render(
    <AnnotatedCodeBlock
      code={code}
      language="typescript"
      highlight={{ startLine: 2, endLine: 2 }}
      followHighlight={false}
      revealKey="a"
    />,
  )
  expect(scrollTo).not.toHaveBeenCalled()
})

const line = 'const answer = 42 // the answer'

it.each([
  { name: 'a match across syntax runs', ranges: [[6, 16]] },
  {
    name: 'two matches in one run',
    ranges: [
      [21, 24],
      [25, 28],
    ],
  },
  {
    name: 'matches in different runs',
    ranges: [
      [0, 2],
      [15, 17],
      [25, 31],
    ],
  },
])('marks $name, and only the current match as current', ({ ranges }) => {
  const { container } = render(
    <AnnotatedCodeBlock
      code={line}
      language="typescript"
      highlight={null}
      followHighlight={false}
      revealKey="a"
      marks={
        new Map([
          [1, ranges.map(([from, to], index) => ({ range: [from!, to!], current: index === 0 }))],
        ])
      }
    />,
  )
  const marks = [...container.querySelectorAll('mark')]
  const text = (current: boolean) =>
    marks
      .filter((mark) => mark.hasAttribute('aria-current') === current)
      .map((mark) => mark.textContent)
      .join('')
  const [[from, to], ...rest] = ranges as [number, number][]
  expect(text(true)).toBe(line.slice(from, to))
  expect(text(false)).toBe(rest.map(([a, b]) => line.slice(a, b)).join(''))
})
