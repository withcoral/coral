/** @vitest-environment happy-dom */
import { act, render } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import { Elapsed } from './elapsed'

const format = (ms: number) => `${Math.floor(ms)} ms`

it('advances locally between snapshots and stops at the server terminal duration', async () => {
  vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'performance'] })
  try {
    const { container, rerender, unmount } = render(
      <Elapsed milliseconds={500} running>
        {format}
      </Elapsed>,
    )
    expect(container.textContent).toBe('500 ms')
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(container.textContent).toBe('2500 ms')
    rerender(
      <Elapsed milliseconds={2700} running>
        {format}
      </Elapsed>,
    )
    expect(container.textContent).toBe('2700 ms')
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000)
    })
    expect(container.textContent).toBe('3700 ms')
    rerender(
      <Elapsed milliseconds={3200} running={false}>
        {format}
      </Elapsed>,
    )
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000)
    })
    expect(container.textContent).toBe('3200 ms')
    unmount()
    expect(vi.getTimerCount()).toBe(0)
  } finally {
    vi.useRealTimers()
  }
})
