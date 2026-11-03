import type { Meta, StoryObj } from '@storybook/react-vite'

import { createRoutesStub, Link } from 'react-router'
import { expect, fn, screen, spyOn, userEvent, waitFor } from 'storybook/test'

import { Button } from '@/wax/components'
import { CopyButton as CopyButtonComponent } from '@/wax/components/button/copy-button'
import { ExternalLink as ExternalLinkComponent } from '@/wax/components/button/external-link'
import { IconButton } from '@/wax/components/button/icon-button'
import { IconLink as IconLinkComponent } from '@/wax/components/button/icon-link'
import { InternalLink as InternalLinkComponent } from '@/wax/components/button/internal-link'
import { TextButton } from '@/wax/components/button/text-button'
import { dismissAllToasts, ToastContainer } from '@/wax/components/toast'
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

export const Outline: Story = {
  render: () => (
    <div style={{ display: 'flex', gap: 12 }}>
      <Button.Container variant="outline">
        <Button.Text>Outline</Button.Text>
      </Button.Container>
      <Button.Container variant="outline">
        <Button.Icon name="Search" />
        <Button.Text>Investigations</Button.Text>
      </Button.Container>
      <Button.Container disabled variant="outline">
        <Button.Text>Disabled</Button.Text>
      </Button.Container>
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

export const Pressed: Story = {
  render: () => (
    <div style={{ display: 'flex', gap: 12 }}>
      <TextButton variant="secondary" aria-pressed={false}>
        Not pressed
      </TextButton>
      <TextButton variant="secondary" aria-pressed={true}>
        Pressed
      </TextButton>
      <TextButton variant="secondary" aria-pressed={true} disabled>
        Disabled
      </TextButton>
      {/* An icon whose shape also shows the state, as the Live toggle's does. */}
      <TextButton variant="secondary" icon="Locate" aria-pressed={false}>
        Live
      </TextButton>
      <TextButton variant="secondary" icon="LocateFixed" aria-pressed={true}>
        Live
      </TextButton>
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
          <Button.Icon name="Trash" />
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
          <Button.Icon name="Trash" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container variant="destructive">
          <Button.Text>Destructive</Button.Text>
        </Button.Container>
        <Button.Container variant="destructive">
          <Button.Icon name="Trash" />
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
          <Button.Icon name="Trash" />
        </Button.Container>
      </div>
      <div style={{ display: 'flex', gap: 12 }}>
        <Button.Container size="36" variant="destructive">
          <Button.Text>Destructive</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="destructive">
          <Button.Icon name="Trash" />
          <Button.Text>Delete</Button.Text>
        </Button.Container>
        <Button.Container size="36" variant="destructive">
          <Button.Text>Remove</Button.Text>
          <Button.Icon name="X" />
        </Button.Container>
        <Button.Container disabled size="36" variant="destructive">
          <Button.Icon name="Trash" />
        </Button.Container>
        <Button.Container disabled size="36" variant="destructive">
          <Button.Text>Disabled</Button.Text>
        </Button.Container>
      </div>
    </div>
  ),
}

// Catches: a busy button that runs its click handler, such as reopening a
// confirmation or starting the same request again.
export const Loading: Story = {
  args: { onClick: fn() },
  render: ({ onClick }) => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {(['primary', 'secondary', 'bare', 'destructive'] as const).map((variant) => (
        <div key={variant} style={{ alignItems: 'center', display: 'flex', gap: 12 }}>
          {(['22', '32', '36'] as const).map((size) => (
            <Button.TextButton
              ariaLabel={`Save ${variant} ${size}`}
              busy
              key={size}
              onClick={onClick}
              size={size}
              variant={variant}
            >
              Save
            </Button.TextButton>
          ))}
        </div>
      ))}
    </div>
  ),
  play: async ({ args, canvas }) => {
    const button = canvas.getByRole('button', { name: 'Save primary 32' })
    await expect(button).toHaveAttribute('aria-busy', 'true')
    await expect(button).toHaveAttribute('aria-disabled', 'true')
    await expect(button).not.toBeDisabled()
    await userEvent.click(button)
    await expect(args.onClick).not.toHaveBeenCalled()
    await expect(canvas.getAllByRole('status')[1]).toHaveTextContent('Loading')
  },
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

export const IconLink: Story = {
  render: () => {
    const Stub = createRoutesStub([
      {
        Component: () => (
          <IconLinkComponent name="ChevronRight" to="/providers" tooltipText="View all providers" />
        ),
        path: '/*',
      },
    ])
    return <Stub />
  },
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

const longPath = 'reports.quarterly.revenueByRegionAndProductLineIncludingRefunds'
/** A link in a narrow cell truncates its text and shows the whole of it on hover. */
export const TruncatedLink: Story = {
  render: () => {
    const Stub = createRoutesStub([
      {
        Component: () => (
          <div style={{ display: 'flex', width: 220 }}>
            <Button.Container as={Link} to="/functions/report" variant="link">
              <Button.Text tooltip={longPath}>{longPath}</Button.Text>
            </Button.Container>
          </div>
        ),
        path: '/*',
      },
    ])
    return <Stub />
  },
  play: async () => {
    await userEvent.hover(screen.getByText(longPath))
    // The popup repeats the whole text beside the truncated link.
    await waitFor(() => expect(screen.getAllByText(longPath)).toHaveLength(2))
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

export const CopyFailure: Story = {
  render: () => (
    <>
      <CopyButtonComponent textToCopy="https://example.com">Copy</CopyButtonComponent>
      <ToastContainer />
    </>
  ),
  beforeEach: () => {
    dismissAllToasts()
    return () => dismissAllToasts()
  },
  play: async ({ canvas }) => {
    const clipboard = spyOn(navigator.clipboard, 'writeText').mockRejectedValue(
      new Error('Clipboard denied'),
    )
    try {
      const button = canvas.getByRole('button', { name: 'Copy to clipboard' })
      await userEvent.click(button)
      // The toast bounces in from zero opacity.
      await waitFor(() => expect(screen.getByText('Copy failed')).toBeVisible())
      await expect(screen.getByText('Select and copy the text manually.')).toBeVisible()
      await expect(button).toHaveAccessibleName('Copy to clipboard')
    } finally {
      clipboard.mockRestore()
    }
  },
}
