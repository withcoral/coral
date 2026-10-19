import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { expect, waitFor, within } from 'storybook/test'

import { Button, Dialog, Menu } from '@/wax/components'
import { Tooltip } from '@/wax/components/tooltip'

import { resizeViewport } from '../../../../.storybook/browser'

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

// Catches: a trigger that drops the tooltip's handlers, so its tooltip never opens.
export const TriggerWithTooltip: Story = {
  play: async ({ canvas, userEvent }) => {
    const trigger = canvas.getByRole('button', { name: 'Add provider' })
    await userEvent.hover(trigger)
    await within(document.body).findByText('Add provider')
    await userEvent.click(trigger)
    await within(document.body).findByRole('dialog', { name: 'Add provider' })
  },
  render: () => (
    <Dialog.Root>
      <Tooltip content="Add provider" delay={0}>
        <Dialog.Trigger
          render={<Button.Container variant="bare" size="32" ariaLabel="Add provider" />}
        >
          <Button.Icon name="Plus" />
        </Dialog.Trigger>
      </Tooltip>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          <Dialog.Title>Add provider</Dialog.Title>
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

/** The z-index an element paints at among the document's portals. */
function layer(element: Element) {
  let z = 0
  for (
    let node: Element | null = element;
    node && node !== document.body;
    node = node.parentElement
  ) {
    const value = Number.parseInt(getComputedStyle(node).zIndex, 10)
    if (!Number.isNaN(value)) z = value
  }
  return z
}

/** Whether a point on the left edge reaches the page, rather than a backdrop or a popup. */
const pageReachable = (canvas: HTMLElement) =>
  canvas.contains(document.elementFromPoint(8, window.innerHeight / 2))

/** Whether the page behind is locked against scrolling, on whichever element scrolls it. */
const scrollLocked = () =>
  [document.documentElement, document.body].some((element) =>
    /hidden|clip/.test(getComputedStyle(element).overflowY),
  )

/** Whether a visible backdrop dims the left edge of the page. */
const dimmed = () =>
  document.elementsFromPoint(8, window.innerHeight / 2).some((element) => {
    const style = getComputedStyle(element)
    return (
      style.position === 'fixed' &&
      Number(style.opacity) > 0 &&
      style.backgroundColor !== 'rgba(0, 0, 0, 0)'
    )
  })

/** Mounts open, like a drawer a route renders, and closes the way a route navigates away. */
function RouteDrawer() {
  const [route, setRoute] = useState<'list' | 'record'>('record')
  const drawer = Dialog.useCloseAfterExit(route === 'record' ? route : null, () => setRoute('list'))
  if (route === 'list')
    return (
      <Button.Container variant="secondary" onClick={() => setRoute('record')}>
        <Button.Text>Open drawer</Button.Text>
      </Button.Container>
    )
  return (
    <Dialog.Root {...drawer}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="xl" placement="right">
          <Dialog.Title>Drawer</Dialog.Title>
          <Dialog.Description>The drawer slides in from its edge.</Dialog.Description>
          <Dialog.Close />
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

export const RightDrawer: Story = {
  play: async ({ canvas, userEvent }) => {
    const body = within(document.body)
    const dialog = await body.findByRole('dialog', { name: 'Drawer' })
    // The reveal plays even though the drawer mounted already open.
    await expect(dialog.getAnimations().length).toBeGreaterThan(0)
    // Unlike a rail, a drawer is modal at every width.
    await waitFor(() => expect(dimmed()).toBe(true))
    await expect(scrollLocked()).toBe(true)
    await userEvent.click(within(dialog).getByRole('button', { name: /close/i }))
    // The route changes only after the exit ends.
    await expect(canvas.queryByRole('button', { name: 'Open drawer' })).toBeNull()
    await expect(await canvas.findByRole('button', { name: 'Open drawer' })).toBeVisible()
    await expect(body.queryByRole('dialog')).toBeNull()
  },
  render: () => <RouteDrawer />,
}

function RailPage() {
  const [record, setRecord] = useState<number | null>(null)
  const [confirming, setConfirming] = useState(false)
  return (
    <div style={{ minHeight: '200vh', padding: 24 }}>
      <Menu.Container>
        <Menu.Trigger render={<Button.Container variant="secondary" />}>
          <Button.Text>Page actions</Button.Text>
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Item>Export</Menu.Item>
        </Menu.Content>
      </Menu.Container>
      {[1, 2, 3].map((value) => (
        <Button.TextButton key={value} variant="secondary" onClick={() => setRecord(value)}>
          {`Record ${value}`}
        </Button.TextButton>
      ))}
      <Dialog.Rail open={record !== null} onOpenChange={(open) => !open && setRecord(null)}>
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup size="xl" placement="right" fullWidthOnMobile>
            <Dialog.Header>
              <Dialog.Title>{`Record ${record}`}</Dialog.Title>
              <Dialog.Close />
            </Dialog.Header>
            <Dialog.Description>Pick another record while this stays open.</Dialog.Description>
            <Tooltip content="The record's hint" delay={0}>
              <Button.TextButton variant="secondary">Hint</Button.TextButton>
            </Tooltip>
            <Button.TextButton variant="destructive" onClick={() => setConfirming(true)}>
              Delete record
            </Button.TextButton>
            <Menu.Container>
              <Menu.Trigger render={<Button.Container variant="secondary" />}>
                <Button.Text>Record actions</Button.Text>
              </Menu.Trigger>
              <Menu.Content>
                <Menu.Item>Duplicate</Menu.Item>
              </Menu.Content>
            </Menu.Container>
            <Dialog.Root open={confirming} onOpenChange={setConfirming}>
              <Dialog.Portal>
                <Dialog.Backdrop />
                <Dialog.Popup>
                  <Dialog.Title>Delete this record?</Dialog.Title>
                  <Dialog.Close />
                </Dialog.Popup>
              </Dialog.Portal>
            </Dialog.Root>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Rail>
      <Button.TextButton variant="secondary">After the rail</Button.TextButton>
    </div>
  )
}

/**
 * A rail beside the page above 640px; a full-screen modal at 640px or less. The
 * play resizes the viewport across the breakpoint while the rail stays open.
 */
export const Rail: Story = {
  parameters: { layout: 'fullscreen' },
  render: () => <RailPage />,
  play: async ({ canvas, canvasElement, userEvent }) => {
    const body = within(document.body)
    await userEvent.click(canvas.getByRole('button', { name: 'Record 1' }))
    const rail = await body.findByRole('dialog', { name: 'Record 1' })
    // Opening from closed focuses the rail.
    await waitFor(() => expect(rail.contains(document.activeElement)).toBe(true))
    await expect(rail).not.toHaveAttribute('aria-modal', 'true')
    await expect(pageReachable(canvasElement)).toBe(true)
    await expect(dimmed()).toBe(false)
    await expect(scrollLocked()).toBe(false)

    // The page stays usable: a click switches the record in the same rail and keeps focus.
    await userEvent.click(canvas.getByRole('button', { name: 'Record 2' }))
    await expect(body.getByRole('dialog', { name: 'Record 2' })).toBe(rail)
    await expect(canvas.getByRole('button', { name: 'Record 2' })).toHaveFocus()
    // Escape outside the rail is not the rail's.
    await userEvent.keyboard('{Escape}')
    await expect(body.getByRole('dialog', { name: 'Record 2' })).toBe(rail)

    // Tab follows the document: from the page into the rail, then on past it.
    await userEvent.click(canvas.getByRole('button', { name: 'Record 3' }))
    await userEvent.tab()
    await expect(within(rail).getByRole('button', { name: 'Close' })).toHaveFocus()
    await userEvent.tab()
    await userEvent.tab()
    await expect(within(rail).getByRole('button', { name: 'Delete record' })).toHaveFocus()
    await userEvent.tab()
    await expect(within(rail).getByRole('button', { name: 'Record actions' })).toHaveFocus()
    await userEvent.tab()
    await expect(canvas.getByRole('button', { name: 'After the rail' })).toHaveFocus()
    await userEvent.tab({ shift: true })
    await expect(within(rail).getByRole('button', { name: 'Record actions' })).toHaveFocus()
    await userEvent.click(canvas.getByRole('button', { name: 'Record 3' }))
    await userEvent.tab()
    await userEvent.tab({ shift: true })
    await expect(canvas.getByRole('button', { name: 'Record 3' })).toHaveFocus()
    // Focus left the rail, and it stays open.
    await expect(body.getByRole('dialog', { name: 'Record 3' })).toBe(rail)

    // A page menu draws above the rail and keeps its Escape.
    await userEvent.click(canvas.getByRole('button', { name: 'Page actions' }))
    const menu = await body.findByRole('menu')
    await expect(layer(menu)).toBeGreaterThan(layer(rail))
    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(body.queryByRole('menu')).toBeNull())
    await expect(body.getByRole('dialog', { name: 'Record 3' })).toBe(rail)

    // A menu opened in the rail is portaled out of it, draws above it and keeps its Escape.
    await userEvent.click(within(rail).getByRole('button', { name: 'Record actions' }))
    const railMenu = await body.findByRole('menu')
    await expect(rail.contains(railMenu)).toBe(false)
    await expect(layer(railMenu)).toBeGreaterThan(layer(rail))
    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(body.queryByRole('menu')).toBeNull())
    await expect(body.getByRole('dialog', { name: 'Record 3' })).toBe(rail)

    // A tooltip in the rail draws above it.
    await userEvent.hover(within(rail).getByRole('button', { name: 'Hint' }))
    await expect(layer(await body.findByText("The record's hint"))).toBeGreaterThan(layer(rail))
    await userEvent.unhover(within(rail).getByRole('button', { name: 'Hint' }))

    // A dialog opened from the rail is modal, above it, with its own backdrop and Escape.
    await userEvent.click(within(rail).getByRole('button', { name: 'Delete record' }))
    const confirm = await body.findByRole('dialog', { name: 'Delete this record?' })
    await expect(layer(confirm)).toBeGreaterThan(layer(rail))
    await waitFor(() => expect(dimmed()).toBe(true))
    await userEvent.keyboard('{Escape}')
    await waitFor(() =>
      expect(body.queryByRole('dialog', { name: 'Delete this record?' })).toBeNull(),
    )
    await expect(body.getByRole('dialog', { name: 'Record 3' })).toBe(rail)

    // At 640px or less the open rail turns modal and takes focus.
    await userEvent.click(canvas.getByRole('button', { name: 'Record 3' }))
    // Storybook's own UI cannot resize its viewport; the rest needs the test runner.
    if (!(await resizeViewport(600, 800))) return
    await waitFor(() => expect(rail.contains(document.activeElement)).toBe(true))
    await expect(body.getByRole('dialog', { name: 'Record 3' })).toBe(rail)
    await expect(canvas.queryByRole('button', { name: 'Record 1' })).toBeNull()
    await expect(pageReachable(canvasElement)).toBe(false)
    await expect(scrollLocked()).toBe(true)

    // Back beside the page, focus stays where it is and the page is usable again.
    const focused = document.activeElement
    await resizeViewport(1200, 900)
    await waitFor(() => expect(canvas.queryByRole('button', { name: 'Record 1' })).not.toBeNull())
    await expect(document.activeElement).toBe(focused)
    await expect(pageReachable(canvasElement)).toBe(true)
    await expect(scrollLocked()).toBe(false)

    // Escape inside the rail still closes it.
    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(body.queryByRole('dialog')).toBeNull())
  },
}
