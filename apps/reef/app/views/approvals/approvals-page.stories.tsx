import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { approval, run, workspace } from '../activity/fixtures'
import { freezeStoryClock, minutesAfterStoryNow } from '../activity/story-clock'
import { ApprovalsPage } from './approvals-page'

const meta = {
  title: 'Approvals/Page',
  component: ApprovalsPage,
  beforeEach: freezeStoryClock,
  parameters: { layout: 'fullscreen', chromatic: { viewports: [390, 1440] } },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <Stub initialEntries={[`/${workspace}/approvals`]} />
        </div>
      )
    },
  ],
} satisfies Meta<typeof ApprovalsPage>
export default meta
type Story = StoryObj<typeof meta>

// An Agent's request, your own, and a Run with no intent, most urgent first.
export const Pending: Story = {
  args: {
    approvals: [
      { action: { ...approval, deadline: minutesAfterStoryNow(5) }, run },
      {
        action: {
          ...approval,
          name: `${workspace}/approvals/calendar`,
          callPath: 'coral.providers.calendar.createEvent',
          deadline: minutesAfterStoryNow(15),
        },
        run: {
          ...run,
          name: `${workspace}/runs/calendar`,
          agent: '',
          actorLabel: 'You',
          intent: 'Book the quarterly review',
        },
      },
      {
        action: {
          ...approval,
          name: `${workspace}/approvals/plain`,
          deadline: minutesAfterStoryNow(45),
        },
        run: { ...run, name: `${workspace}/runs/plain`, agent: '', actorLabel: 'You', intent: '' },
      },
    ],
  },
}

export const Empty: Story = { args: { approvals: [] } }
