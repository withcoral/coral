/** @vitest-environment happy-dom */

import { act } from 'react'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, expect, it, vi } from 'vitest'

import { DocumentSubmitButton } from './document-submit-button'

afterEach(() => {
  vi.restoreAllMocks()
})

function submit(form: HTMLFormElement, submitter: HTMLButtonElement) {
  const event = new SubmitEvent('submit', { bubbles: true, cancelable: true, submitter })
  form.dispatchEvent(event)
  return event
}

it('shows progress only on the submitter and keeps its name and value successful', async () => {
  render(
    <form aria-label="Decision">
      <DocumentSubmitButton name="intent" value="decline">
        Deny
      </DocumentSubmitButton>
      <DocumentSubmitButton name="intent" value="approve">
        Approve
      </DocumentSubmitButton>
    </form>,
  )
  const form = screen.getByRole('form', { name: 'Decision' }) as HTMLFormElement
  const deny = screen.getByRole('button', { name: 'Deny' }) as HTMLButtonElement
  const approve = screen.getByRole('button', { name: 'Approve' }) as HTMLButtonElement

  await act(async () => submit(form, approve))

  expect(approve).toHaveAttribute('aria-busy', 'true')
  expect(approve).toHaveAttribute('aria-disabled', 'true')
  expect(approve).not.toBeDisabled()
  expect(deny).not.toHaveAttribute('aria-busy')
  expect(new FormData(form, approve).get('intent')).toBe('approve')
  expect(submit(form, approve).defaultPrevented).toBe(true)
})

it.each<[string, React.ComponentProps<'form'>, { formTarget?: string }]>([
  ['another handler cancels', { onSubmit: (event) => event.preventDefault() }, {}],
  ['its form opens in another window', { target: '_blank' }, {}],
  ['its submitter opens in another window', {}, { formTarget: '_blank' }],
])('ignores a submission that %s', async (_case, formProps, buttonProps) => {
  render(
    <form aria-label="Example" {...formProps}>
      <DocumentSubmitButton {...buttonProps}>Save</DocumentSubmitButton>
    </form>,
  )
  const form = screen.getByRole('form', { name: 'Example' }) as HTMLFormElement
  const save = screen.getByRole('button', { name: 'Save' }) as HTMLButtonElement

  await act(async () => submit(form, save))

  expect(save).not.toHaveAttribute('aria-busy')
})

it('uses the browser-selected first submit button when Enter submits the form', async () => {
  const user = userEvent.setup()
  render(
    <form aria-label="Keyboard">
      <input aria-label="Name" />
      <DocumentSubmitButton>Save first</DocumentSubmitButton>
      <DocumentSubmitButton>Save second</DocumentSubmitButton>
    </form>,
  )

  await user.click(screen.getByRole('textbox', { name: 'Name' }))
  await user.keyboard('{Enter}')

  expect(screen.getByRole('button', { name: 'Save first' })).toHaveAttribute('aria-busy', 'true')
  expect(screen.getByRole('button', { name: 'Save second' })).not.toHaveAttribute('aria-busy')
})

it('clears progress when the page returns from the back-forward cache', async () => {
  render(
    <form aria-label="Cached">
      <DocumentSubmitButton>Save</DocumentSubmitButton>
    </form>,
  )
  const form = screen.getByRole('form', { name: 'Cached' }) as HTMLFormElement
  const save = screen.getByRole('button', { name: 'Save' }) as HTMLButtonElement
  await act(async () => submit(form, save))
  expect(save).toHaveAttribute('aria-busy', 'true')

  const restored = new Event('pageshow') as PageTransitionEvent
  Object.defineProperty(restored, 'persisted', { value: true })
  await act(async () => window.dispatchEvent(restored))

  expect(save).not.toHaveAttribute('aria-busy')
  expect(save).not.toHaveAttribute('aria-disabled')
})
