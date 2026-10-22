/** @vitest-environment happy-dom */

import { render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'

import * as Dialog from './index'

function open(title: React.ReactNode) {
  render(
    <Dialog.Root open>
      <Dialog.Portal>
        <Dialog.Popup>{title}</Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>,
  )
}

// Catches: a custom `hidden` prop that swallows the native attribute, so a title meant to be
// gone still names the dialog, or one meant to name it is removed from the page.
it('names the dialog with a visually hidden title, and keeps native hidden as HTML hides it', () => {
  open(<Dialog.Title visuallyHidden>Activity details</Dialog.Title>)
  expect(screen.getByRole('dialog', { name: 'Activity details' })).toBeInTheDocument()
  expect(screen.getByText('Activity details')).not.toHaveAttribute('hidden')
})

it('passes the native hidden attribute through', () => {
  open(<Dialog.Title hidden>Gone</Dialog.Title>)
  expect(screen.getByText('Gone')).toHaveAttribute('hidden')
})
