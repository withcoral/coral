/** @vitest-environment happy-dom */
import { act, render } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import { Deadline } from './deadline'

it('wakes only when its label changes: each minute, each second in the last minute, then never', async () => {
  vi.useFakeTimers({ now: 0 })
  try {
    const { container } = render(<Deadline at={new Date(90_500).toISOString()} />)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(container.textContent).toBe('Expires in 2 minutes')
    await act(async () => {
      await vi.advanceTimersToNextTimerAsync()
    })
    expect(Date.now()).toBe(30_500)
    expect(container.textContent).toBe('Expires in 60 seconds')
    await act(async () => {
      await vi.advanceTimersToNextTimerAsync()
    })
    expect(Date.now()).toBe(31_500)
    expect(container.textContent).toBe('Expires in 59 seconds')
    const wakes: number[] = []
    while (vi.getTimerCount() > 0) {
      await act(async () => {
        await vi.advanceTimersToNextTimerAsync()
      })
      wakes.push(Date.now())
    }
    // One wake per visible second, and none after the deadline.
    expect(wakes).toEqual(Array.from({ length: 59 }, (_, index) => 32_500 + index * 1000))
    expect(container.textContent).toBe('Deadline reached. Check the current state.')
  } finally {
    vi.useRealTimers()
  }
})
