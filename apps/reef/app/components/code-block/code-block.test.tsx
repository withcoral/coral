import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import { HighlightedCode } from './code-block'

describe('HighlightedCode', () => {
  it('renders source code as text rather than HTML', async () => {
    const code = `select '<script data-injected>alert("unsafe")</script>'`
    const screen = await render(<HighlightedCode code={code} language="sql" />)

    expect(screen.container.textContent).toBe(code)
    expect(screen.container.querySelector('script')).toBeNull()
  })
})
