import type { Meta, StoryObj } from '@storybook/react-vite'

import { Button, Inputs, Typography } from '@/wax/components'

import { Container } from './container'

const meta = {
  component: Container,
  title: 'Wax/Card/Container',
  parameters: { layout: 'centered' },
} satisfies Meta<typeof Container>
export default meta
type Story = StoryObj<typeof meta>

export const LinkedContent: Story = {
  render: () => (
    <Container as="a" href="#recording" interactive>
      <Typography.BodyStrong>Organization Owner · Rotate credential</Typography.BodyStrong>
      <Typography.BodySmall>Interaction recording · 12.4s</Typography.BodySmall>
    </Container>
  ),
}

export const CustomContent: Story = {
  render: () => (
    <Container as="section" aria-label="Workspace settings">
      <Typography.HeadingSmall as="h2">Workspace settings</Typography.HeadingSmall>
      <Typography.Body as="p">Card framing is shared with summary cards.</Typography.Body>
      <Typography.BodySmallStrong as="label" htmlFor="workspace-name">
        Workspace name
      </Typography.BodySmallStrong>
      <Inputs.TextInput id="workspace-name" defaultValue="Operations team" />
      <div>
        <Button.TextButton>Save name</Button.TextButton>
      </div>
    </Container>
  ),
}
