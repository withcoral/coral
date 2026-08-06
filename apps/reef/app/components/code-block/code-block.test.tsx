import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import { HighlightedCode } from './code-block'

describe('HighlightedCode', () => {
  it.each(['plain', 'sql'] as const)(
    'renders %s source as text rather than HTML',
    async (language) => {
      const code = `select '<script data-injected>alert("unsafe")</script>'`
      const screen = await render(<HighlightedCode code={code} language={language} />)

      expect(screen.container.textContent).toBe(code)
      expect(screen.container.querySelector('script')).toBeNull()
    },
  )

  it('renders plain text without applying SQL token classes', async () => {
    const code = 'SELECT customers FROM Zurich'
    const screen = await render(<HighlightedCode code={code} language="plain" />)

    expect(screen.container.textContent).toBe(code)
    expect(screen.container.querySelector('.token')).toBeNull()
  })
})
