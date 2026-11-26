import type { Meta, StoryObj } from '@storybook/react-vite'

import { Typography } from '@/wax/components'
import { TextLink } from './text-link'

const meta = {
  component: TextLink,
  title: 'Wax/TextLink',
  args: { children: 'Read the docs', href: 'https://example.com', external: true },
  render: (args) => (
    <Typography.Body>
      No secret stores defined. <TextLink {...args} />.
    </Typography.Body>
  ),
} satisfies Meta<typeof TextLink>
export default meta
type Story = StoryObj<typeof meta>

export const InBody: Story = {}
export const InBodySmall: Story = {
  render: (args) => (
    <Typography.BodySmall>
      Coral sends anonymous usage data. <TextLink {...args} />.
    </Typography.BodySmall>
  ),
}
