/** @vitest-environment happy-dom */
import { render, screen } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'

import { Body } from './body'
import { Cell } from './cell'
import { Container } from './container'
import { Row } from './row'

afterEach(() => vi.restoreAllMocks())

const renderCapped = (maxRows: number) =>
  render(
    <Container ariaLabel="Requests" columns={[{ label: 'Name' }]} layout="fixed" maxRows={maxRows}>
      <Body>
        <Row>
          <Cell>Linear</Cell>
        </Row>
      </Body>
    </Container>,
  )

// Catches: a cap that shows part of a row, the partial row maxRows exists to prevent.
it.each([0, -1, 1.5, Infinity, NaN])('rejects maxRows %s', (maxRows) => {
  vi.spyOn(console, 'error').mockImplementation(() => {})
  expect(() => renderCapped(maxRows)).toThrow(
    new RangeError('Table maxRows must be a positive integer.'),
  )
})

it('renders the rows under a positive whole-number cap', () => {
  renderCapped(3)
  expect(screen.getByRole('cell', { name: 'Linear' })).toBeInTheDocument()
})
