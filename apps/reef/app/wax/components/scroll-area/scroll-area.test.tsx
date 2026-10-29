/** @vitest-environment happy-dom */

import { act, render, screen } from '@testing-library/react'
import { createRef } from 'react'
import { expect, it, vi } from 'vitest'

import { Container } from './scroll-area'

it('keeps the outer ref distinct from the reading viewport without leaking reading props into markup', () => {
  const outer = createRef<HTMLDivElement>()
  const port = createRef<HTMLDivElement>()
  const { container: host, unmount } = render(
    <Container
      ref={outer}
      viewportRef={port}
      readingKey="inbox"
      readingPaused
      readingFollowStart
      revealRecord="approval"
    >
      <article data-record-key="approval">Review this tool</article>
    </Container>,
  )
  expect(outer.current).not.toBe(port.current)
  expect(outer.current?.contains(port.current)).toBe(true)
  expect(port.current?.querySelector('article')?.dataset.recordKey).toBe('approval')
  expect(screen.queryByRole('table')).not.toBeInTheDocument()
  expect(host.innerHTML).not.toMatch(/readingkey|readingpaused|readingfollowstart|revealrecord/i)
  unmount()
  expect(outer.current).toBeNull()
  expect(port.current).toBeNull()
})

it('reports viewport start changes only for active reading, not concealed or ordinary content', () => {
  const port = createRef<HTMLDivElement>()
  const onStartChange = vi.fn()
  const view = (readingKey: string | undefined, readingPaused = false) => (
    <Container
      viewportRef={port}
      readingKey={readingKey}
      readingPaused={readingPaused}
      onReadingStartChange={onStartChange}
    >
      <article data-record-key="approval">Review this tool</article>
    </Container>
  )
  const { rerender } = render(view('inbox'))
  // Visibility is the only layout input this event test needs. Browser stories cover geometry.
  Object.defineProperties(port.current!, {
    clientHeight: { value: 240 },
    clientWidth: { value: 300 },
  })
  const rect = new DOMRect(0, 0, 300, 240)
  vi.spyOn(port.current!, 'getClientRects').mockReturnValue(
    Object.assign([rect], { item: (index: number) => (index === 0 ? rect : null) }),
  )
  const scroll = (top: number) =>
    act(() => {
      port.current!.scrollTop = top
      port.current!.dispatchEvent(new Event('scroll'))
    })
  scroll(20)
  expect(onStartChange).toHaveBeenLastCalledWith(false)
  scroll(0)
  expect(onStartChange).toHaveBeenLastCalledWith(true)

  rerender(view('inbox', true))
  onStartChange.mockClear()
  scroll(20)
  expect(onStartChange).not.toHaveBeenCalled()

  rerender(view('inbox'))
  // Restoration suppresses incidental scroll events until the next animation frame.
  act(() => port.current!.dispatchEvent(new WheelEvent('wheel', { deltaY: 20 })))
  expect(onStartChange).toHaveBeenLastCalledWith(false)

  rerender(view(undefined))
  onStartChange.mockClear()
  scroll(0)
  expect(onStartChange).not.toHaveBeenCalled()
})

it('preserves the native viewport ref when the content owns its own scroll port', () => {
  const port = createRef<HTMLDivElement>()
  render(<Container viewportRef={port} renderViewport={<textarea aria-label="Program" />} />)
  expect(port.current?.tagName).toBe('TEXTAREA')
  expect(port.current).toHaveAttribute('aria-label', 'Program')
})
