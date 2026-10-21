/** @vitest-environment happy-dom */
import { act, cleanup, render, screen } from '@testing-library/react'
import { createRoutesStub, useLoaderData } from 'react-router'
import { afterEach, expect, it, vi } from 'vitest'
import { useRevalidateWhileVisible } from './use-revalidate-while-visible'

afterEach(() => {
  cleanup()
  vi.useRealTimers()
  vi.restoreAllMocks()
})

function Page() {
  useRevalidateWhileVisible(30_000)
  return <p>{`Read ${useLoaderData<number>()}`}</p>
}

async function renderPage() {
  let reads = 0
  const Stub = createRoutesStub([{ path: '/', Component: Page, loader: () => ++reads }])
  render(<Stub />)
  await screen.findByText('Read 1')
  // The page listens once its effects run.
  await act(async () => {})
}

const visibility = (state: DocumentVisibilityState) =>
  vi.spyOn(document, 'visibilityState', 'get').mockReturnValue(state)

// Catches: numbers that stay as the page first loaded them, or reads from a tab nobody sees.
it.each([
  [
    'the tab becomes visible',
    () => document.dispatchEvent(new Event('visibilitychange')),
    'Read 2',
  ],
  ['the window gets focus', () => window.dispatchEvent(new Event('focus')), 'Read 2'],
  ['the interval passes', () => vi.advanceTimersByTime(30_000), 'Read 2'],
  ['less than the interval passes', () => vi.advanceTimersByTime(29_000), 'Read 1'],
])('reads again when %s', async (_, event, expected) => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  await renderPage()
  await act(async () => event())
  expect(await screen.findByText(expected)).toBeTruthy()
})

it('reads nothing while the tab is hidden', async () => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  await renderPage()
  visibility('hidden')
  await act(async () => {
    window.dispatchEvent(new Event('focus'))
    vi.advanceTimersByTime(90_000)
  })
  expect(screen.getByText('Read 1')).toBeTruthy()
})
