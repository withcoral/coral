import type { Meta, StoryObj } from '@storybook/react-vite'

import { createRoutesStub } from 'react-router'

import { Button } from '@/wax/components'
import { CopyButton as CopyButtonComponent } from '@/wax/components/button/copy-button'
import { ExternalLink as ExternalLinkComponent } from '@/wax/components/button/external-link'
import { IconButton } from '@/wax/components/button/icon-button'
import { InternalLink as InternalLinkComponent } from '@/wax/components/button/internal-link'
import { TextButton } from '@/wax/components/button/text-button'
import { darkTheme } from '@/wax/theme/theme-dark.css'

const meta = {
  component: Button.Container,
  decorators: [
    (Story) => (
      <div className={darkTheme} style={{ padding: 24 }}>
        <Story />
      </div>
    ),
  ],
  parameters: {
    backgrounds: { default: 'dark' },
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/Button',
} satisfies Meta<typeof Button.Container>

export default meta
type Story = StoryObj<typeof meta>

export const Primary: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container size="22" variant="primary">
          <Button.Text>Primary</Button.Text>
        </Button.Container>
        <Button.Container size="22" variant="primary">
          <Button.Icon name="Search" />
          <Button.Text>Investigations</Button.Text>
        </Button.Container>
        <Button.Container size="22" variant="primary">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled size="22" variant="primary">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
        <Button.Container disabled size="22" variant="primary">
          <Button.Icon name="Plus" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container variant="primary">
          <Button.Text>Primary</Button.Text>
        </Button.Container>
        <Button.Container variant="primary">
          <Button.Icon name="Search" />
          <Button.Text>Investigations</Button.Text>
        </Button.Container>
        <Button.Container variant="primary">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled variant="primary">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
        <Button.Container disabled variant="primary">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container size="36" variant="primary">
          <Button.Text>Primary</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="primary">
          <Button.Icon name="Search" />
          <Button.Text>Investigations</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="primary">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled size="36" variant="primary">
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled size="36" variant="primary">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
      </div>
    </div>
  ),
}

export const Secondary: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container size="22" variant="secondary">
          <Button.Text>Secondary</Button.Text>
        </Button.Container>
        <Button.Container size="22" variant="secondary">
          <Button.Icon name="Search" />
          <Button.Text>Investigations</Button.Text>
        </Button.Container>
        <Button.Container size="22" variant="secondary">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled size="22" variant="secondary">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
        <Button.Container disabled size="22" variant="secondary">
          <Button.Icon name="Plus" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container variant="secondary">
          <Button.Text>Secondary</Button.Text>
        </Button.Container>
        <Button.Container variant="secondary">
          <Button.Icon name="Search" />
          <Button.Text>Investigations</Button.Text>
        </Button.Container>
        <Button.Container variant="secondary">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled variant="secondary">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
        <Button.Container disabled variant="secondary">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container size="36" variant="secondary">
          <Button.Text>Secondary</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="secondary">
          <Button.Icon name="Search" />
          <Button.Text>Investigations</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="secondary">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled size="36" variant="secondary">
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled size="36" variant="secondary">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
      </div>
    </div>
  ),
}

export const Bare: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container size="22" variant="bare">
          <Button.Text>Bare</Button.Text>
        </Button.Container>
        <Button.Container size="22" variant="bare">
          <Button.Icon name="Search" />
          <Button.Text>Investigations</Button.Text>
        </Button.Container>
        <Button.Container size="22" variant="bare">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled size="22" variant="bare">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
        <Button.Container disabled size="22" variant="bare">
          <Button.Icon name="Plus" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container variant="bare">
          <Button.Text>Bare</Button.Text>
        </Button.Container>
        <Button.Container variant="bare">
          <Button.Icon name="Search" />
          <Button.Text>Investigations</Button.Text>
        </Button.Container>
        <Button.Container variant="bare">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled variant="bare">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
        <Button.Container disabled variant="bare">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container size="36" variant="bare">
          <Button.Text>Bare</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="bare">
          <Button.Icon name="Search" />
          <Button.Text>Investigations</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="bare">
          <Button.Text>Add</Button.Text>
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled size="36" variant="bare">
          <Button.Icon name="Plus" />
        </Button.Container>
        <Button.Container disabled size="36" variant="bare">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
      </div>
    </div>
  ),
}

export const Destructive: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container size="22" variant="destructive">
          <Button.Text>Destructive</Button.Text>
        </Button.Container>
        <Button.Container size="22" variant="destructive">
          <Button.Icon name="Trash2" />
          <Button.Text>Delete</Button.Text>
        </Button.Container>
        <Button.Container size="22" variant="destructive">
          <Button.Text>Remove</Button.Text>
          <Button.Icon name="X" />
        </Button.Container>
        <Button.Container disabled size="22" variant="destructive">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
        <Button.Container disabled size="22" variant="destructive">
          <Button.Icon name="Trash2" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container variant="destructive">
          <Button.Text>Destructive</Button.Text>
        </Button.Container>
        <Button.Container variant="destructive">
          <Button.Icon name="Trash2" />
          <Button.Text>Delete</Button.Text>
        </Button.Container>
        <Button.Container variant="destructive">
          <Button.Text>Remove</Button.Text>
          <Button.Icon name="X" />
        </Button.Container>
        <Button.Container disabled variant="destructive">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
        <Button.Container disabled variant="destructive">
          <Button.Text>Delete</Button.Text>
          <Button.Icon name="Trash2" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container size="36" variant="destructive">
          <Button.Text>Destructive</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="destructive">
          <Button.Icon name="Trash2" />
          <Button.Text>Delete</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="destructive">
          <Button.Text>Remove</Button.Text>
          <Button.Icon name="X" />
        </Button.Container>
        <Button.Container disabled size="36" variant="destructive">
          <Button.Icon name="Trash2" />
        </Button.Container>
        <Button.Container disabled size="36" variant="destructive">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
      </div>
    </div>
  ),
}

export const Text: Story = {
  render: () => <TextButton>Hello</TextButton>,
}

export const Icon: Story = {
  render: () => <IconButton name="Settings" tooltipText="Settings" />,
}

export const AsLink: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Button.Container as="a" href="https://example.com" target="_blank" variant="primary">
        <Button.Text>Link Button</Button.Text>
      </Button.Container>
      <Button.Container
        as="a"
        disabled
        href="https://example.com"
        target="_blank"
        variant="primary"
      >
        <Button.Text>Disabled Link Button</Button.Text>
      </Button.Container>
      <Button.Container as="a" href="https://example.com" target="_blank" variant="secondary">
        <Button.Icon name="ExternalLink" />
        <Button.Text>External Link</Button.Text>
      </Button.Container>
      <Button.Container
        as="a"
        disabled
        href="https://example.com"
        target="_blank"
        variant="secondary"
      >
        <Button.Icon name="ExternalLink" />
        <Button.Text>Disabled External Link</Button.Text>
      </Button.Container>
    </div>
  ),
}

export const ExternalLink: Story = {
  render: () => (
    <div style={{ display: 'flex', gap: 48 }}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <span style={{ color: '#999', fontSize: 12 }}>Primary variant</span>
        <ExternalLinkComponent href="https://example.com" size="large" variant="primary">
          Slack
        </ExternalLinkComponent>
        <ExternalLinkComponent href="https://example.com" variant="primary">
          Slack
        </ExternalLinkComponent>
        <ExternalLinkComponent href="https://example.com" size="small" variant="primary">
          Slack
        </ExternalLinkComponent>
        <span style={{ color: '#999', fontSize: 12, marginTop: 8 }}>Disabled</span>
        <ExternalLinkComponent disabled href="https://example.com" size="large" variant="primary">
          Slack
        </ExternalLinkComponent>
        <ExternalLinkComponent disabled href="https://example.com" variant="primary">
          Slack
        </ExternalLinkComponent>
        <ExternalLinkComponent disabled href="https://example.com" size="small" variant="primary">
          Slack
        </ExternalLinkComponent>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <span style={{ color: '#999', fontSize: 12 }}>Subtle variant</span>
        <ExternalLinkComponent href="https://example.com" size="large" variant="subtle">
          Slack
        </ExternalLinkComponent>
        <ExternalLinkComponent href="https://example.com" variant="subtle">
          Slack
        </ExternalLinkComponent>
        <ExternalLinkComponent href="https://example.com" size="small" variant="subtle">
          Slack
        </ExternalLinkComponent>
        <span style={{ color: '#999', fontSize: 12, marginTop: 8 }}>Disabled</span>
        <ExternalLinkComponent disabled href="https://example.com" size="large" variant="subtle">
          Slack
        </ExternalLinkComponent>
        <ExternalLinkComponent disabled href="https://example.com" variant="subtle">
          Slack
        </ExternalLinkComponent>
        <ExternalLinkComponent disabled href="https://example.com" size="small" variant="subtle">
          Slack
        </ExternalLinkComponent>
      </div>
    </div>
  ),
}

export const InternalLink: Story = {
  render: () => {
    const Stub = createRoutesStub([
      {
        Component: () => (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <span style={{ color: '#999', fontSize: 12 }}>Size default</span>
            <InternalLinkComponent to="/investigations">Investigations</InternalLinkComponent>
            <span style={{ color: '#999', fontSize: 12, marginTop: 8 }}>Size small</span>
            <InternalLinkComponent size="small" to="/alerts">
              Alerts
            </InternalLinkComponent>
            <span style={{ color: '#999', fontSize: 12, marginTop: 8 }}>Disabled</span>
            <InternalLinkComponent disabled to="/investigations">
              Investigations
            </InternalLinkComponent>
            <InternalLinkComponent disabled to="/alerts">
              Alerts
            </InternalLinkComponent>
          </div>
        ),
        path: '/*',
      },
    ])
    return <Stub />
  },
}

export const CopyButton: Story = {
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', flexDirection: 'column', gap: 16 }}>
      <span style={{ color: '#999', fontSize: 12 }}>Icon only (default)</span>
      <CopyButtonComponent textToCopy="https://example.com" />
      <span style={{ color: '#999', fontSize: 12, marginTop: 8 }}>Custom icon (Link)</span>
      <CopyButtonComponent iconName="Link" textToCopy="https://example.com" />
      <span style={{ color: '#999', fontSize: 12, marginTop: 8 }}>With text</span>
      <CopyButtonComponent textToCopy="https://example.com">
        <Button.Text>Copy</Button.Text>
      </CopyButtonComponent>
    </div>
  ),
}
