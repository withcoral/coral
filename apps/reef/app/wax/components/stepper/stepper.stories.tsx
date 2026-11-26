import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'

import { Button, Inputs, Typography } from '@/wax/components'

import { Stepper } from './stepper'

const meta = {
  component: Stepper,
  title: 'Components/Stepper',
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return (
        <div style={{ maxWidth: 640 }}>
          <Stub initialEntries={['/']} />
        </div>
      )
    },
  ],
  args: { label: 'Add provider', steps: [] },
} satisfies Meta<typeof Stepper>
export default meta
type Story = StoryObj<typeof meta>

const nameStep = (
  <>
    <Typography.Body as="p">
      Use coral.providers.github to call this provider's tools.
    </Typography.Body>
    <Inputs.TextInput ariaLabel="Program name" defaultValue="github" />
    <div>
      <Button.TextButton>Add provider</Button.TextButton>
    </div>
  </>
)

export const PersistentRail: Story = {
  args: {
    layout: 'rail',
    steps: [
      { title: 'Source', state: 'done', summary: 'https://api.github.com/openapi.json' },
      {
        title: 'Configuration',
        state: 'done',
        summary: 'OAuth authorization code · 1 server',
        change: { to: '/configuration' },
      },
      { title: 'Account', state: 'current', content: nameStep },
      { title: 'Review', state: 'upcoming' },
    ],
  },
}

export const FirstStep: Story = {
  args: {
    steps: [
      { title: 'Choose a provider source', state: 'current', content: nameStep },
      { title: 'Whose account does it use?', state: 'upcoming' },
      { title: 'Choose a program name', state: 'upcoming' },
    ],
  },
}
export const MiddleStep: Story = {
  args: {
    steps: [
      {
        title: 'Choose a provider source',
        state: 'done',
        summary: 'https://api.github.com/openapi.json',
        change: { to: '/change' },
      },
      { title: 'Whose account does it use?', state: 'current', content: nameStep },
      { title: 'Choose a program name', state: 'upcoming' },
    ],
  },
}
export const LastStep: Story = {
  args: {
    steps: [
      {
        title: 'Choose a provider source',
        state: 'done',
        summary: 'Uploaded file',
        change: { to: '/change' },
      },
      {
        title: 'Whose account does it use?',
        state: 'done',
        summary: 'Signed in as the shared account',
      },
      { title: 'Choose a program name', state: 'current', content: nameStep },
    ],
  },
}
