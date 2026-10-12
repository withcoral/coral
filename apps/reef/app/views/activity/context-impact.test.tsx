/** @vitest-environment happy-dom */
import { render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import { ContextImpact } from './context-impact'

it.each([
  ['-1', '−1 B', '1 B saved'],
  ['999', '+999 B', '999 B added'],
  ['1000', '+1 kB', '1 kB added'],
  // Rounds before it picks the unit, so this is not "1,000 kB".
  ['999500', '+1 MB', '1 MB added'],
  ['9223372036854775807', '+9.22 EB', '9.22 EB added'],
  ['0', '0 B', 'No net context change'],
  [undefined, '—', 'Context impact unavailable'],
])('shows %s bytes with a sign and an accessible meaning', (bytes, label, meaning) => {
  render(<ContextImpact bytes={bytes} />)
  expect(screen.getByLabelText(meaning).textContent).toBe(label)
})

it('keeps exact byte counts in the tooltip when compact context values are rounded', () => {
  render(<ContextImpact bytes="9223372036854775807" />)
  const title = screen.getByLabelText('9.22 EB added').getAttribute('title')
  expect(title).toContain('9,223,372,036,854,775,807 bytes added')
  expect(title).toContain('Parent totals use exact byte counts.')
})

it('shows a partial total while work runs and says the total will change', () => {
  render(<ContextImpact bytes="1000" partial />)
  const impact = screen.getByLabelText('1 kB added so far')
  expect(impact.textContent).toBe('+1 kB')
  expect(impact.getAttribute('title')).toMatch(/^Some work is still running\. This total changes/)
})
