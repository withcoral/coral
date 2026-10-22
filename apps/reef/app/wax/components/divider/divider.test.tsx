/** @vitest-environment happy-dom */
import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it, vi } from 'vitest'
import { Divider } from './divider'

// happy-dom does not implement pointer capture; the component only calls it, never reads it back.
Element.prototype.setPointerCapture ??= vi.fn()
Element.prototype.releasePointerCapture ??= vi.fn()

function markup(onResize: (deltaY: number) => void, percent = 50) {
  return render(<Divider label="Resize panes" onResize={onResize} percent={percent} />)
}

it('reports its split share on the ARIA separator', () => {
  markup(vi.fn(), 37)
  const separator = screen.getByRole('separator', { name: 'Resize panes' })
  expect(separator).toHaveAttribute('aria-orientation', 'horizontal')
  expect(separator).toHaveAttribute('aria-valuenow', '37')
})

it('reports a step delta on ↓, a bigger one with Shift, and the reverse on ↑', async () => {
  const onResize = vi.fn()
  markup(onResize)
  const user = userEvent.setup()
  const separator = screen.getByRole('separator', { name: 'Resize panes' })
  separator.focus()
  await user.keyboard('{ArrowDown}')
  expect(onResize).toHaveBeenLastCalledWith(16)
  await user.keyboard('{ArrowUp}')
  expect(onResize).toHaveBeenLastCalledWith(-16)
  await user.keyboard('{Shift>}{ArrowDown}{/Shift}')
  expect(onResize).toHaveBeenLastCalledWith(48)
})

it('reports a pointer drag, and stops once the drag is cancelled', () => {
  const onResize = vi.fn()
  markup(onResize)
  const separator = screen.getByRole('separator', { name: 'Resize panes' })
  fireEvent.pointerDown(separator, { pointerId: 1, clientY: 100 })
  fireEvent.pointerMove(separator, { pointerId: 1, clientY: 112 })
  expect(onResize).toHaveBeenLastCalledWith(12)
  fireEvent.pointerCancel(separator, { pointerId: 1 })
  fireEvent.pointerMove(separator, { pointerId: 1, clientY: 140 })
  expect(onResize).toHaveBeenCalledTimes(1)
})
