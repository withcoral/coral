/** @vitest-environment happy-dom */

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'

import { CredentialInput } from './credential-input'

it('shows a hidden value on request and keeps what was typed', async () => {
  const { container } = render(<CredentialInput id="key" name="key" masked />)
  // A password input has no role, so it is found by its name.
  const input = container.querySelector('input[name="key"]')!
  await userEvent.type(input, 'typed-secret')
  expect(input).toHaveAttribute('type', 'password')

  await userEvent.click(screen.getByRole('button', { name: 'Show value' }))
  expect(input).toHaveAttribute('type', 'text')
  expect(input).toHaveValue('typed-secret')

  await userEvent.click(screen.getByRole('button', { name: 'Hide value' }))
  expect(input).toHaveAttribute('type', 'password')
  expect(input).toHaveValue('typed-secret')
})

it('starts a value that needs no masking visible', () => {
  render(<CredentialInput id="key" name="key" masked={false} />)
  expect(screen.getByRole('textbox')).toHaveAttribute('name', 'key')
  expect(screen.getByRole('button', { name: 'Hide value' })).toBeInTheDocument()
})
