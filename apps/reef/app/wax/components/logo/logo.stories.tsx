import type { Meta, StoryObj } from '@storybook/react-vite'

import { expect } from 'storybook/test'

import { Card } from '@/wax/components/card/card'

import { Logo } from './logo'

// A dark glyph on a transparent background, like Notion's logo.
const transparentLogo = `data:image/svg+xml,${encodeURIComponent(
  '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M4 3h4l8 12V3h4v18h-4L8 9v12H4z" fill="#000"/></svg>',
)}`
// The same glyph in white, for the dark theme.
const transparentLogoDark = transparentLogo.replace(
  encodeURIComponent('#000'),
  encodeURIComponent('#fff'),
)
// A logo that fills its own square background.
const fullBleedLogo = `data:image/svg+xml,${encodeURIComponent(
  '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="24" height="24" rx="5" fill="#4A154B"/><circle cx="12" cy="12" r="5" fill="#fff"/></svg>',
)}`

const meta = {
  component: Logo,
  parameters: { layout: 'centered' },
  tags: ['autodocs'],
  title: 'Wax/Logo',
} satisfies Meta<typeof Logo>

export default meta
type Story = StoryObj<typeof meta>

export const Sizes: Story = {
  args: { icons: { light: transparentLogo }, name: 'Notion' },
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', gap: 16 }}>
      <Logo name="Notion" icons={{ light: transparentLogo }} size="40" />
      <Logo name="Slack" icons={{ light: fullBleedLogo }} size="40" />
      <Logo name="Linear" size="40" />
      <Logo name="Notion" icons={{ light: transparentLogo }} />
      <Logo name="Slack" icons={{ light: fullBleedLogo }} />
      <Logo name="Linear" />
      <Logo name="Notion" icons={{ light: transparentLogo }} size="20" />
      <Logo name="Slack" icons={{ light: fullBleedLogo }} size="20" />
      <Logo name="Linear" size="20" />
      <Logo name="Notion" icons={{ light: transparentLogo }} size="18" />
      <Logo name="Slack" icons={{ light: fullBleedLogo }} size="18" />
      <Logo name="Linear" size="18" />
    </div>
  ),
}

/** A dark glyph brings a white version, so it stays legible in both themes. */
export const OnCard: Story = {
  args: { icons: { dark: transparentLogoDark, light: transparentLogo }, name: 'Notion' },
  globals: { themeComparison: true },
  render: (args) => (
    <div style={{ width: 320 }}>
      <Card
        title={args.name}
        description="Search and read pages in your workspace."
        icon={<Logo {...args} />}
      />
    </div>
  ),
}

/** When the image fails to load, the first letter stands in. */
export const BrokenImage: Story = {
  args: { icons: { light: '/logo-that-does-not-exist.svg' }, name: 'Datadog' },
  play: async ({ canvas }) => {
    await expect(await canvas.findByText('D')).toBeVisible()
  },
}
