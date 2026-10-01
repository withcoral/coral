import type { Meta, StoryObj } from '@storybook/react-vite'

import { Typography } from '@/wax/components'

import { ProviderLogo } from './provider-logo'

const meta = {
  component: ProviderLogo,
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Components/Sources/ProviderLogo',
} satisfies Meta<typeof ProviderLogo>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    name: 'github',
    size: 'medium',
  },
}

export const Sizes = {
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', gap: 18 }}>
      <LogoExample name="github" size="small" />
      <LogoExample name="gmail" size="medium" />
      <LogoExample name="unknown-source" size="large" />
    </div>
  ),
} satisfies StoryObj

function LogoExample({
  name,
  size,
}: {
  name: string
  size: React.ComponentProps<typeof ProviderLogo>['size']
}) {
  return (
    <div style={{ alignItems: 'center', display: 'flex', flexDirection: 'column', gap: 8 }}>
      <ProviderLogo name={name} size={size} />
      <Typography.BodySmall variant="tertiary">{size}</Typography.BodySmall>
    </div>
  )
}
