/** @vitest-environment happy-dom */
import { act, renderHook } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import { useCloseAfterExit } from './use-close-after-exit'

type Drawer = ReturnType<typeof useCloseAfterExit>
/** The record the route opens, or null for none. */
type Step = { route: string | null } | ((drawer: Drawer) => void)

const requestClose: Step = (drawer) => drawer.onOpenChange(false)
const exitEnds: Step = (drawer) => drawer.onOpenChangeComplete(false)
const reopen: Step = (drawer) => void drawer.reopen()

it.each<[string, Step[], { open: boolean; closes: number }]>([
  // Navigating first would unmount the drawer before its exit plays.
  [
    'a close request hides the drawer but waits for the exit',
    [requestClose],
    { open: false, closes: 0 },
  ],
  ['the end of the exit closes once', [requestClose, exitEnds], { open: false, closes: 1 }],
  // Back or a link already closed it; a second navigation would eat a history entry.
  [
    'an exit after the route closed does not close again',
    [requestClose, { route: null }, exitEnds],
    { open: false, closes: 0 },
  ],
  // A stale closing state would keep the drawer shut for good.
  [
    'the drawer opens again after the route closed it',
    [requestClose, exitEnds, { route: null }, { route: 'first' }],
    { open: true, closes: 1 },
  ],
  // The route moves straight to another record, so it never passes through closed.
  [
    'selecting another record while closing opens it and stays there',
    [requestClose, { route: 'second' }, exitEnds],
    { open: true, closes: 0 },
  ],
  // A click on the record that is closing keeps it, so no close may follow.
  [
    'a reopen during the exit keeps the drawer open',
    [requestClose, reopen, exitEnds],
    { open: true, closes: 0 },
  ],
  [
    'a reopen after the exit keeps the drawer open',
    [requestClose, exitEnds, reopen],
    { open: true, closes: 1 },
  ],
])('%s', (_, steps, expected) => {
  const onClose = vi.fn()
  const { result, rerender } = renderHook(({ route }) => useCloseAfterExit(route, onClose), {
    initialProps: { route: 'first' as string | null },
  })
  for (const step of steps) {
    if (typeof step === 'function') act(() => step(result.current))
    else rerender(step)
  }
  expect(result.current.open).toBe(expected.open)
  expect(onClose).toHaveBeenCalledTimes(expected.closes)
})

// Catches: a caller that cannot tell whether the close already navigated away,
// and so cannot cancel that navigation.
it.each<[string, Step[], boolean]>([
  ['before the exit ends', [requestClose], false],
  ['after the exit ends', [requestClose, exitEnds], true],
  ['with no close under way', [], false],
])('reopen says whether the close navigated, %s', (_, steps, navigated) => {
  const { result } = renderHook(() => useCloseAfterExit('first', vi.fn()))
  for (const step of steps) if (typeof step === 'function') act(() => step(result.current))
  let reported: boolean | undefined
  act(() => {
    reported = result.current.reopen()
  })
  expect(reported).toBe(navigated)
  expect(result.current.open).toBe(true)
})
