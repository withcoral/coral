/** @vitest-environment happy-dom */
import { act, cleanup, render, screen } from '@testing-library/react'
import { createRoutesStub, useLoaderData, useRevalidator } from 'react-router'
import { afterEach, expect, it, vi } from 'vitest'
import { useRevalidateWhileVisible } from './use-revalidate-while-visible'

afterEach(() => {
  cleanup()
  vi.useRealTimers()
  vi.restoreAllMocks()
})

function Page() {
  useRevalidateWhileVisible(30_000)
  const { revalidate } = useRevalidator()
  return (
    <>
      <p>{`Read ${useLoaderData<number>()}`}</p>
      {/* A reload the router starts on its own, as after an action. */}
      <button onClick={() => void revalidate()}>Reload</button>
    </>
  )
}

/** Renders the page. Each read waits for `gate` when one is given. */
async function renderPage(gate?: Promise<void>) {
  let reads = 0
  const loader = async () => {
    const read = ++reads
    if (read > 1) await gate
    return read
  }
  const Stub = createRoutesStub([{ path: '/', Component: Page, loader }])
  render(<Stub />)
  await screen.findByText('Read 1')
  // The page listens once its effects run.
  await act(async () => {})
}

const visibility = (state: DocumentVisibilityState) =>
  vi.spyOn(document, 'visibilityState', 'get').mockReturnValue(state)

const focus = () => window.dispatchEvent(new Event('focus'))
const visibilityChange = () => document.dispatchEvent(new Event('visibilitychange'))

// Catches: numbers that stay as the page first loaded them, or a reload restarted by the
// event that comes with it.
it.each([
  ['the tab becomes visible', visibilityChange],
  ['the window gets focus', focus],
  ['the interval passes', () => vi.advanceTimersByTime(30_000)],
  [
    'focus and a visibility change arrive together, once',
    () => {
      visibilityChange()
      focus()
    },
  ],
])('reads again when %s', async (_, event) => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  await renderPage()
  visibility('visible')
  await act(async () => event())
  expect(await screen.findByText('Read 2')).toBeTruthy()
  await act(async () => {})
  expect(screen.queryByText('Read 3')).toBeNull()
})

// Catches: reads before the interval, or from a tab nobody sees.
it.each([
  ['less than the interval passes', 'visible', () => vi.advanceTimersByTime(29_000)],
  ['the tab turns hidden', 'hidden', visibilityChange],
  ['a hidden tab gets focus', 'hidden', focus],
  ['the interval passes in a hidden tab', 'hidden', () => vi.advanceTimersByTime(90_000)],
] as const)('reads nothing when %s', async (_, state, event) => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  await renderPage()
  visibility(state)
  await act(async () => event())
  expect(screen.getByText('Read 1')).toBeTruthy()
})

// Catches: a refresh that restarts a reload the router already runs.
it('waits for a reload the router started', async () => {
  let open!: () => void
  await renderPage(new Promise<void>((resolve) => (open = resolve)))
  visibility('visible')
  await act(async () => screen.getByRole('button', { name: 'Reload' }).click())
  await act(async () => focus())
  await act(async () => open())
  expect(await screen.findByText('Read 2')).toBeTruthy()
  await act(async () => {})
  expect(screen.queryByText('Read 3')).toBeNull()
})
