// @vitest-environment happy-dom
import { render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'

import { filterQuery } from '@withcoral/wax/utils/filter-query'

import { App } from './app'

it('renders Wax components with their class names', () => {
  render(<App />)
  expect(screen.getByRole('button', { name: 'Save' }).className).toMatch(/button_variant_primary/)
})

it('reads a helper that moved into the package', () => {
  expect(filterQuery(' SAV ').matches('Save')).toBe(true)
})
