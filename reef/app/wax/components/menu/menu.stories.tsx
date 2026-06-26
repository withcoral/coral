import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { MemoryRouter } from 'react-router'
import { expect, screen, waitFor } from 'storybook/test'

import { Button } from '@/wax/components'

import * as Menu from './index'

const meta: Meta<typeof Menu.Container> = {
  component: Menu.Container,
  decorators: [
    (Story) => (
      <MemoryRouter>
        <Story />
      </MemoryRouter>
    ),
  ],
  title: 'Wax/Menu',
}

export default meta
type Story = StoryObj<typeof Menu.Container>

export const Default: Story = {
  render: () => (
    <div style={{ padding: '100px' }}>
      <Menu.Container>
        <Menu.Trigger render={<Button.Container variant="secondary" />}>
          <Button.Icon name="Ellipsis" />
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Item icon="Pencil" onClick={() => alert('Edit clicked')}>
            Edit
          </Menu.Item>
          <Menu.Item icon="Copy" onClick={() => alert('Copy clicked')}>
            Copy
          </Menu.Item>
          <Menu.Separator />
          <Menu.Item icon="Trash" onClick={() => alert('Delete clicked')}>
            Delete
          </Menu.Item>
        </Menu.Content>
      </Menu.Container>
    </div>
  ),
}

export const WithLinks: Story = {
  render: () => (
    <div style={{ padding: '100px' }}>
      <Menu.Container>
        <Menu.Trigger render={<Button.Container variant="secondary" />}>
          <Button.Icon name="Ellipsis" />
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Item icon="Home" to="/">
            Home
          </Menu.Item>
          <Menu.Item icon="Users" to="/profile">
            Profile
          </Menu.Item>
          <Menu.Item icon="Settings" to="/settings">
            Settings
          </Menu.Item>
        </Menu.Content>
      </Menu.Container>
    </div>
  ),
}

export const WithGroups: Story = {
  render: () => (
    <div style={{ padding: '100px' }}>
      <Menu.Container>
        <Menu.Trigger render={<Button.Container variant="secondary" />}>
          <Button.Icon name="Ellipsis" />
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Group>
            <Menu.GroupLabel>Edit</Menu.GroupLabel>
            <Menu.Item icon="Pencil" onClick={() => alert('Edit clicked')}>
              Edit
            </Menu.Item>
            <Menu.Item icon="Copy" onClick={() => alert('Copy clicked')}>
              Copy
            </Menu.Item>
          </Menu.Group>
          <Menu.Separator />
          <Menu.Group>
            <Menu.GroupLabel>Actions</Menu.GroupLabel>
            <Menu.Item icon="Link" onClick={() => alert('Share clicked')}>
              Share
            </Menu.Item>
            <Menu.Item icon="Trash" onClick={() => alert('Delete clicked')}>
              Delete
            </Menu.Item>
          </Menu.Group>
        </Menu.Content>
      </Menu.Container>
    </div>
  ),
}

export const MixedActionsAndLinks: Story = {
  render: () => (
    <div style={{ padding: '100px' }}>
      <Menu.Container>
        <Menu.Trigger render={<Button.Container variant="secondary" />}>
          <Button.Icon name="Ellipsis" />
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Group>
            <Menu.GroupLabel>Navigate</Menu.GroupLabel>
            <Menu.Item icon="Home" to="/">
              Home
            </Menu.Item>
            <Menu.Item icon="Users" to="/profile">
              Profile
            </Menu.Item>
          </Menu.Group>
          <Menu.Separator />
          <Menu.Group>
            <Menu.GroupLabel>Actions</Menu.GroupLabel>
            <Menu.Item icon="Copy" onClick={() => alert('Copied!')}>
              Copy link
            </Menu.Item>
            <Menu.Item icon="Trash" onClick={() => alert('Deleted!')}>
              Delete
            </Menu.Item>
          </Menu.Group>
        </Menu.Content>
      </Menu.Container>
    </div>
  ),
}

export const WithSubmenu: Story = {
  render: () => (
    <div style={{ padding: '100px' }}>
      <Menu.Container>
        <Menu.Trigger render={<Button.Container variant="secondary" />}>
          <Button.Icon name="Ellipsis" />
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Item icon="Pencil" onClick={() => alert('Edit clicked')}>
            Edit this very long item name that should be truncated
          </Menu.Item>
          <Menu.Submenu>
            <Menu.SubmenuTrigger icon="Link">
              Share this item with your team members
            </Menu.SubmenuTrigger>
            <Menu.SubmenuContent>
              <Menu.Item icon="Link" onClick={() => alert('Copy link')}>
                Copy shareable link to clipboard
              </Menu.Item>
              <Menu.Item icon="AtSign" onClick={() => alert('Email')}>
                Send via email notification
              </Menu.Item>
              <Menu.Item icon="MessageCircle" onClick={() => alert('Slack')}>
                Slack
              </Menu.Item>
            </Menu.SubmenuContent>
          </Menu.Submenu>
          <Menu.Submenu>
            <Menu.SubmenuTrigger icon="Download">Export</Menu.SubmenuTrigger>
            <Menu.SubmenuContent>
              <Menu.Item onClick={() => alert('Export as PDF')}>PDF</Menu.Item>
              <Menu.Item onClick={() => alert('Export as CSV')}>CSV</Menu.Item>
              <Menu.Item onClick={() => alert('Export as JSON')}>JSON</Menu.Item>
            </Menu.SubmenuContent>
          </Menu.Submenu>
          <Menu.Separator />
          <Menu.Item icon="Trash" onClick={() => alert('Delete clicked')}>
            Delete
          </Menu.Item>
        </Menu.Content>
      </Menu.Container>
    </div>
  ),
}

export const WithNestedSubmenu: Story = {
  render: () => (
    <div style={{ padding: '100px' }}>
      <Menu.Container>
        <Menu.Trigger render={<Button.Container variant="secondary" />}>
          <Button.Icon name="Ellipsis" />
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Item icon="Pencil" onClick={() => alert('Edit clicked')}>
            Edit
          </Menu.Item>
          <Menu.Submenu>
            <Menu.SubmenuTrigger icon="Link">Share</Menu.SubmenuTrigger>
            <Menu.SubmenuContent>
              <Menu.Item icon="Link" onClick={() => alert('Copy link')}>
                Copy link
              </Menu.Item>
              <Menu.Submenu>
                <Menu.SubmenuTrigger icon="Users">Share with team</Menu.SubmenuTrigger>
                <Menu.SubmenuContent>
                  <Menu.Item onClick={() => alert('Engineering')}>Engineering</Menu.Item>
                  <Menu.Item onClick={() => alert('Design')}>Design</Menu.Item>
                  <Menu.Item onClick={() => alert('Product')}>Product</Menu.Item>
                </Menu.SubmenuContent>
              </Menu.Submenu>
              <Menu.Submenu>
                <Menu.SubmenuTrigger icon="Globe">Share externally</Menu.SubmenuTrigger>
                <Menu.SubmenuContent>
                  <Menu.Item icon="AtSign" onClick={() => alert('Email')}>
                    Email
                  </Menu.Item>
                  <Menu.Item icon="MessageCircle" onClick={() => alert('Slack')}>
                    Slack
                  </Menu.Item>
                </Menu.SubmenuContent>
              </Menu.Submenu>
            </Menu.SubmenuContent>
          </Menu.Submenu>
          <Menu.Separator />
          <Menu.Item icon="Trash" onClick={() => alert('Delete clicked')}>
            Delete
          </Menu.Item>
        </Menu.Content>
      </Menu.Container>
    </div>
  ),
}

export const KeyboardEscClosesSubmenusOneAtATime: Story = {
  play: async ({ canvas, step, userEvent }) => {
    await step('Open menu', async () => {
      // Ensure menu is closed before starting (handles rerun without remount)
      for (let i = 0; i < 5 && screen.queryByRole('menu'); i++) {
        await userEvent.keyboard('{Escape}')
      }
      await waitFor(() => expect(screen.queryByRole('menu')).toBeNull())

      await userEvent.click(canvas.getByRole('button'))
      await waitFor(() => expect(screen.getByRole('menuitem', { name: 'Edit' })).toBeVisible())
    })

    await step('Navigate to deepest submenu', async () => {
      await userEvent.keyboard('{ArrowDown}') // Edit
      await userEvent.keyboard('{ArrowDown}') // Share
      await userEvent.keyboard('{ArrowRight}') // Open Share submenu
      await waitFor(() => expect(screen.getByRole('menuitem', { name: 'Copy link' })).toBeVisible())

      await userEvent.keyboard('{ArrowDown}') // Share with team
      await userEvent.keyboard('{ArrowRight}') // Open "Share with team" submenu
      await waitFor(() =>
        expect(screen.getByRole('menuitem', { name: 'Engineering' })).toBeVisible(),
      )
    })

    await step('First ESC closes only the innermost submenu', async () => {
      await userEvent.keyboard('{Escape}')
      await waitFor(() =>
        expect(screen.queryByRole('menuitem', { name: 'Engineering' })).toBeNull(),
      )
      await waitFor(() => expect(screen.getByRole('menuitem', { name: 'Copy link' })).toBeVisible())
    })

    await step('Second ESC closes the Share submenu', async () => {
      await userEvent.keyboard('{Escape}')
      await waitFor(() => expect(screen.queryByRole('menuitem', { name: 'Copy link' })).toBeNull())
      await waitFor(() => expect(screen.getByRole('menuitem', { name: 'Edit' })).toBeVisible())
    })

    await step('Third ESC closes the main menu', async () => {
      await userEvent.keyboard('{Escape}')
      await waitFor(() => expect(screen.queryByRole('menuitem', { name: 'Edit' })).toBeNull())
    })
  },
  render: () => (
    <div style={{ padding: '100px' }}>
      <Menu.Container>
        <Menu.Trigger render={<Button.Container variant="secondary" />}>
          <Button.Icon name="Ellipsis" />
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Item icon="Pencil" onClick={() => alert('Edit clicked')}>
            Edit
          </Menu.Item>
          <Menu.Submenu>
            <Menu.SubmenuTrigger icon="Link">Share</Menu.SubmenuTrigger>
            <Menu.SubmenuContent>
              <Menu.Item icon="Link" onClick={() => alert('Copy link')}>
                Copy link
              </Menu.Item>
              <Menu.Submenu>
                <Menu.SubmenuTrigger icon="Users">Share with team</Menu.SubmenuTrigger>
                <Menu.SubmenuContent>
                  <Menu.Item onClick={() => alert('Engineering')}>Engineering</Menu.Item>
                  <Menu.Item onClick={() => alert('Design')}>Design</Menu.Item>
                  <Menu.Item onClick={() => alert('Product')}>Product</Menu.Item>
                </Menu.SubmenuContent>
              </Menu.Submenu>
              <Menu.Submenu>
                <Menu.SubmenuTrigger icon="Globe">Share externally</Menu.SubmenuTrigger>
                <Menu.SubmenuContent>
                  <Menu.Item icon="AtSign" onClick={() => alert('Email')}>
                    Email
                  </Menu.Item>
                  <Menu.Item icon="MessageCircle" onClick={() => alert('Slack')}>
                    Slack
                  </Menu.Item>
                </Menu.SubmenuContent>
              </Menu.Submenu>
            </Menu.SubmenuContent>
          </Menu.Submenu>
          <Menu.Separator />
          <Menu.Item icon="Trash" onClick={() => alert('Delete clicked')}>
            Delete
          </Menu.Item>
        </Menu.Content>
      </Menu.Container>
    </div>
  ),
}

export const WithRadioGroup: Story = {
  render: function Render() {
    const [theme, setTheme] = useState<'dark' | 'light' | 'system'>('system')

    return (
      <div style={{ padding: '100px' }}>
        <Menu.Container>
          <Menu.Trigger render={<Button.Container variant="secondary" />}>
            <Button.Icon name="Settings" />
            <Button.Text>Settings</Button.Text>
          </Menu.Trigger>
          <Menu.Content>
            <Menu.Item icon="Users" to="/profile">
              Profile
            </Menu.Item>
            <Menu.Submenu>
              <Menu.SubmenuTrigger icon="SunMoon">Appearance</Menu.SubmenuTrigger>
              <Menu.SubmenuContent>
                <Menu.RadioGroup onValueChange={setTheme} value={theme}>
                  <Menu.RadioItem closeOnClick={false} value="system">
                    Use system settings
                  </Menu.RadioItem>
                  <Menu.RadioItem closeOnClick={false} value="light">
                    Light
                  </Menu.RadioItem>
                  <Menu.RadioItem closeOnClick={false} value="dark">
                    Dark
                  </Menu.RadioItem>
                </Menu.RadioGroup>
              </Menu.SubmenuContent>
            </Menu.Submenu>
            <Menu.Separator />
            <Menu.Item icon="LogOut" onClick={() => alert('Sign out')}>
              Sign out
            </Menu.Item>
          </Menu.Content>
        </Menu.Container>
        <p style={{ color: 'white', marginTop: '16px' }}>Selected theme: {theme}</p>
      </div>
    )
  },
}
