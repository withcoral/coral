import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'

import { Button, Dialog } from '@/wax/components'

const meta: Meta = {
  parameters: {
    layout: 'centered',
  },
  title: 'Wax/Dialog',
}

export default meta
type Story = StoryObj

export const Default: Story = {
  play: async ({ canvas, userEvent }) => {
    await userEvent.click((await canvas.findAllByRole('button', { name: 'Delete memory' }))?.[0])
  },
  render: () => (
    <Dialog.Root>
      <Dialog.Trigger render={<Button.Container variant="secondary" />}>
        <Button.Text>Delete memory</Button.Text>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          <Dialog.Title>Delete memory</Dialog.Title>
          <Dialog.Description>
            Are you sure you want to permanently delete this memory?
          </Dialog.Description>
          <Dialog.Actions>
            <Button.TextButton variant="secondary">Cancel</Button.TextButton>
            <Button.Container variant="destructive">
              <Button.Icon name="Trash" />
              <Button.Text>Delete memory</Button.Text>
            </Button.Container>
          </Dialog.Actions>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  ),
}

export const Large: Story = {
  render: () => (
    <Dialog.Root>
      <Dialog.Trigger render={<Button.Container variant="secondary" />}>
        <Button.Text>Open Large Dialog</Button.Text>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          <Dialog.Title>Large Dialog</Dialog.Title>
          <Dialog.Description>
            This dialog uses size=&quot;l&quot; for a wider layout, suitable for forms or content
            that needs more horizontal space.
          </Dialog.Description>
          <Dialog.Close />
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  ),
}

export const ExtraLarge: Story = {
  render: () => (
    <Dialog.Root>
      <Dialog.Trigger render={<Button.Container variant="secondary" />}>
        <Button.Text>Open Extra Large Dialog</Button.Text>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="xl">
          <Dialog.Title>Extra Large Dialog</Dialog.Title>
          <Dialog.Description>
            This dialog uses size=&quot;xl&quot; for the widest layout, suitable for complex forms,
            tables, or content that requires maximum horizontal space.
          </Dialog.Description>
          <Dialog.Close />
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  ),
}

export const Controlled: Story = {
  render: function ControlledDialog() {
    const [open, setOpen] = useState(false)
    return (
      <>
        <Button.Container onClick={() => setOpen(true)} variant="secondary">
          <Button.Text>Open Controlled Dialog</Button.Text>
        </Button.Container>
        <Dialog.Root onOpenChange={setOpen} open={open}>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup>
              <Dialog.Title>Controlled Dialog</Dialog.Title>
              <Dialog.Description>This dialog is controlled via external state.</Dialog.Description>
              <Dialog.Close />
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
      </>
    )
  },
}

export const LongText: Story = {
  render: () => (
    <Dialog.Root>
      <Dialog.Trigger render={<Button.Container variant="secondary" />}>
        <Button.Text>Open Dialog with Long Text</Button.Text>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup>
          <Dialog.Title>
            ThisIsAVeryLongTitleWithNoSpacesThatShouldTestTextOverflowBehaviorAndEnsureItDoesNotBreakTheLayout
          </Dialog.Title>
          <Dialog.Description>
            This is a very long description with multiple paragraphs to test overflow behavior.
            Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor
            incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud
            exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure
            dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
            Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt
            mollit anim id est laborum.
          </Dialog.Description>
          <Dialog.Close />
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  ),
}

export const Confirmation: Story = {
  render: function ConfirmationDialog() {
    const [dialogOpen, setDialogOpen] = useState(false)
    const [confirmOpen, setConfirmOpen] = useState(false)
    const [hasChanges, setHasChanges] = useState(false)

    const handleDialogOpenChange = (open: boolean) => {
      if (!open && hasChanges) {
        setConfirmOpen(true)
      } else {
        setDialogOpen(open)
      }
    }

    const handleDiscard = () => {
      setConfirmOpen(false)
      setHasChanges(false)
      setDialogOpen(false)
    }

    return (
      <>
        <Button.Container onClick={() => setDialogOpen(true)} variant="secondary">
          <Button.Text>Edit Profile</Button.Text>
        </Button.Container>
        <Dialog.Root onOpenChange={handleDialogOpenChange} open={dialogOpen}>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup size="l">
              <Dialog.Title>Edit Profile</Dialog.Title>
              <Dialog.Description>
                Make changes to your profile. Click outside or press Escape to close.
              </Dialog.Description>
              <Dialog.Close />
              <div style={{ marginTop: '16px' }}>
                <input
                  onChange={(e) => setHasChanges(e.target.value.length > 0)}
                  placeholder="Display name"
                  style={{ outline: 'none', padding: '8px', width: '100%' }}
                  type="text"
                />
              </div>
              <Dialog.Root onOpenChange={setConfirmOpen} open={confirmOpen}>
                <Dialog.Portal>
                  <Dialog.Popup>
                    <Dialog.Title>Discard changes?</Dialog.Title>
                    <Dialog.Description>
                      You have unsaved changes that will be lost.
                    </Dialog.Description>
                    <Dialog.Close />
                    <Dialog.Actions>
                      <Button.Container onClick={() => setConfirmOpen(false)} variant="secondary">
                        <Button.Text>Keep editing</Button.Text>
                      </Button.Container>
                      <Button.Container onClick={handleDiscard} variant="destructive">
                        <Button.Text>Discard</Button.Text>
                      </Button.Container>
                    </Dialog.Actions>
                  </Dialog.Popup>
                </Dialog.Portal>
              </Dialog.Root>
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
      </>
    )
  },
}
