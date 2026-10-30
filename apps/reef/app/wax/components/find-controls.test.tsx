/** @vitest-environment happy-dom */
import { fireEvent, render, screen } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import { FindControls } from './find-controls'

// Catches: the Enter that commits IME input, such as Japanese or Chinese, also stepping to
// the next match; or Enter and Shift+Enter no longer stepping once the text is committed.
it.each([
  ['Enter', {}, [[1]]],
  ['Shift+Enter', { shiftKey: true }, [[-1]]],
  ['Enter that commits IME input', { isComposing: true }, []],
  ['Enter an IME reports as key 229', { keyCode: 229 }, []],
])('steps on %s as it should', (_, init, steps) => {
  const onStep = vi.fn()
  render(
    <FindControls
      label="Source"
      query="a"
      onQueryChange={() => {}}
      status=""
      count={2}
      onStep={onStep}
    />,
  )
  fireEvent.keyDown(screen.getByRole('searchbox', { name: 'Find in Source' }), {
    key: 'Enter',
    ...init,
  })
  expect(onStep.mock.calls).toEqual(steps)
})
