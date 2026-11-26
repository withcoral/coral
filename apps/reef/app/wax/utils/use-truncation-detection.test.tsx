/** @vitest-environment happy-dom */
import { render, screen } from '@testing-library/react'
import { useState } from 'react'
import { afterEach, expect, it, vi } from 'vitest'
import { useTruncationDetection } from './use-truncation-detection'

afterEach(() => {
  vi.restoreAllMocks()
})

function Label({ mounted }: { mounted: boolean }) {
  const [element, setElement] = useState<HTMLElement | null>(null)
  const isTruncated = useTruncationDetection(element)
  return (
    <>
      <output>{isTruncated ? 'truncated' : 'fits'}</output>
      {mounted && <span ref={setElement}>A long label</span>}
    </>
  )
}

// A ref object never changes, so an element that mounts after the first effect was never observed.
it('detects truncation on an element that mounts after the first render', () => {
  vi.spyOn(HTMLElement.prototype, 'scrollWidth', 'get').mockReturnValue(200)
  vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockReturnValue(100)

  const { rerender } = render(<Label mounted={false} />)
  expect(screen.getByRole('status')).toHaveTextContent('fits')

  rerender(<Label mounted />)
  expect(screen.getByRole('status')).toHaveTextContent('truncated')
})
