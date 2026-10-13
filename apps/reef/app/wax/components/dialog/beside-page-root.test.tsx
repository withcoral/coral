/** @vitest-environment happy-dom */
import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useState } from 'react'
import { expect, it, onTestFinished, vi } from 'vitest'
import * as Dialog from '.'

/** Resizes the window, as a reader who narrows it or turns a tablet does. */
function resize(width: number) {
  const { happyDOM } = window as unknown as {
    happyDOM: { setViewport: (viewport: { width: number }) => void }
  }
  const { innerWidth } = window
  onTestFinished(() => happyDOM.setViewport({ width: innerWidth }))
  return act(async () => happyDOM.setViewport({ width }))
}

/** A modal confirmation the drawer's content opens, as Cancel run does. */
function Confirmation() {
  const [open, setOpen] = useState(false)
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>
        Cancel run
      </button>
      <Dialog.Root open={open} onOpenChange={setOpen}>
        <Dialog.Portal>
          <Dialog.Backdrop data-testid="confirmation-backdrop" />
          <Dialog.Popup>
            <Dialog.Title>Cancel this run?</Dialog.Title>
            <Dialog.Close ariaLabel="Keep the run" />
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </>
  )
}

function renderDrawer() {
  const onOpenChange = vi.fn()
  render(
    <>
      <button type="button">Page control</button>
      <Dialog.BesidePageRoot open onOpenChange={onOpenChange}>
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup placement="right" fullWidthOnMobile>
            <Dialog.Title>Record</Dialog.Title>
            <Dialog.Close />
            <Confirmation />
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.BesidePageRoot>
    </>,
  )
  return { user: userEvent.setup(), onOpenChange }
}

// Catches: a drawer beside the page that closes when the reader uses the page,
// or that hides the page from assistive technology.
it('above 640px the page stays usable and the drawer stays open', async () => {
  const { user, onOpenChange } = renderDrawer()
  const drawer = await screen.findByRole('dialog', { name: 'Record' })
  expect(drawer).not.toHaveAttribute('aria-modal')
  await user.click(screen.getByRole('button', { name: 'Page control' }))
  expect(screen.getByRole('button', { name: 'Page control' })).toHaveFocus()
  expect(onOpenChange).not.toHaveBeenCalled()
  // Escape still closes it, even from the page.
  await user.keyboard('{Escape}')
  expect(onOpenChange).toHaveBeenCalledWith(false, expect.anything())
})

// Catches: a full-screen drawer that leaves the page it covers reachable, a
// drawer that closes when the mode changes, or one that takes focus from the
// page when it stops being modal.
it('at 640px or less the drawer is modal, and crossing switches the mode while open', async () => {
  const { user, onOpenChange } = renderDrawer()
  const drawer = await screen.findByRole('dialog', { name: 'Record' })
  await user.click(screen.getByRole('button', { name: 'Page control' }))
  await resize(640)
  expect(screen.queryByRole('button', { name: 'Page control' })).toBeNull()
  expect(drawer).toHaveFocus()
  await resize(641)
  expect(screen.getByRole('button', { name: 'Page control' })).toBeInTheDocument()
  expect(drawer).toHaveFocus()
  expect(onOpenChange).not.toHaveBeenCalled()
})

// Catches: a confirmation inside the drawer that inherits the drawer's mode,
// so it loses its backdrop beside the page, or that takes the drawer's place
// as the popup to focus when the drawer becomes modal.
it('a confirmation inside the drawer stays a modal dialog of its own', async () => {
  const { user } = renderDrawer()
  const drawer = await screen.findByRole('dialog', { name: 'Record' })
  await user.click(screen.getByRole('button', { name: 'Cancel run' }))
  await screen.findByRole('dialog', { name: 'Cancel this run?' })
  expect(screen.getByTestId('confirmation-backdrop')).toBeInTheDocument()
  await user.click(screen.getByRole('button', { name: 'Keep the run' }))
  await vi.waitFor(() =>
    expect(screen.queryByRole('dialog', { name: 'Cancel this run?' })).toBeNull(),
  )
  await user.click(screen.getByRole('button', { name: 'Page control' }))
  await resize(640)
  expect(drawer).toHaveFocus()
})
