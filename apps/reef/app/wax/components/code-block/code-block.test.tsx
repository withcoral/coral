/** @vitest-environment happy-dom */
import { render } from '@testing-library/react'
import { expect, it } from 'vitest'
import { CodeBlock } from './code-block'
import * as styles from './code-block.css'

const SOURCE = `// Find stale issues.
export default async function stale(input: Input<{ days?: number }> = {}) {
  const note = \`<script>alert(\${input.days ?? 14})</script>\`
  return /a\\/b/.test(note) ? null : load(note, true)
}`

function tokens(container: HTMLElement, kind: keyof typeof styles.token) {
  return [...container.querySelectorAll(`.${styles.token[kind]}`)].map((node) => node.textContent)
}

it('colours TypeScript by syntax without changing a character of the source', () => {
  const { container } = render(<CodeBlock value={SOURCE} format="typescript" />)
  expect(container.querySelector('pre')!.textContent).toBe(SOURCE)
  expect(container.querySelector('script')).toBeNull()
  expect(tokens(container, 'comment')).toEqual(['// Find stale issues.'])
  expect(tokens(container, 'keyword')).toEqual(
    expect.arrayContaining(['export', 'default', 'async', 'function', 'const', 'return']),
  )
  expect(tokens(container, 'type')).toEqual(['Input', 'number'])
  expect(tokens(container, 'function')).toEqual(['stale', 'test', 'load'])
  expect(tokens(container, 'constant')).toEqual(['14', 'null', 'true'])
  expect(tokens(container, 'string')).toContain('/a\\/b/')
})

it('colours reformatted JSON values and leaves keys and punctuation in the text colour', () => {
  const { container } = render(
    <CodeBlock value={'{"id":9007199254740993,"ok":true,"name":"</pre>"}'} format="json" />,
  )
  expect(container.querySelector('pre')!.textContent).toBe(`{
  "id": 9007199254740993,
  "ok": true,
  "name": "</pre>"
}`)
  expect(tokens(container, 'constant')).toEqual(['9007199254740993', 'true'])
  expect(tokens(container, 'string')).toEqual(['"</pre>"'])
})

it('renders text as given, with no syntax colour', () => {
  const { container } = render(<CodeBlock value={SOURCE} />)
  expect(container.querySelector('pre')!.textContent).toBe(SOURCE)
  expect(container.querySelectorAll('pre span')).toHaveLength(0)
})
