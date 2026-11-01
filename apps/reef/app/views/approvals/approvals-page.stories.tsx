import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { ActionSignalContext } from '@/lib/use-action-signal'
import {
  approval,
  approvalContext,
  approvalRequest,
  invocationRow,
  run,
  workspace,
} from '../activity/fixtures'
import { freezeStoryClock, minutesAfterStoryNow } from '../activity/story-clock'
import { ApprovalsPage } from './approvals-page'

const path = `/${workspace}`
const MAIL_SEND = 'coral.providers.mail.send'

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
    workspacePath: path,
    providers: [
      { callRoot: 'coral.providers.mail', displayName: 'Mail' },
      { callRoot: 'coral.providers.calendar', displayName: 'Calendar' },
    ],
    approvals: [
      {
        action: { ...approval, callPath: MAIL_SEND, deadline: minutesAfterStoryNow(5) },
        run,
      },
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

const reading = {
  detail: {
    ...approvalRequest,
    callPath: MAIL_SEND,
    providerLabel: 'Mail',
    deadline: minutesAfterStoryNow(5),
  },
  context: approvalContext,
  call: invocationRow(run.name, 'one', {
    agent: run.agent,
    sourceLocation: { document: { kind: 'program' }, line: 1, column: 14 },
  }),
}

// The review beside the list: a pending request, then one that someone else settled.
export const Reviewing: Story = {
  args: { ...Pending.args, selected: approval.name, review: reading },
}

export const SettledInReview: Story = {
  args: {
    ...Pending.args,
    approvals: Pending.args.approvals.slice(1),
    selected: approval.name,
    review: {
      ...reading,
      detail: {
        ...reading.detail,
        state: 'declined',
        actionable: false,
        approver: 'someone',
        approverLabel: 'Ada Lovelace',
        decidedAt: minutesAfterStoryNow(-2),
        comment: 'Wrong recipient.',
      },
    },
  },
}

export const RequestUnavailable: Story = {
  args: { ...Pending.args, selected: `${workspace}/approvals/hidden` },
}

export const Empty: Story = { args: { workspacePath: path, approvals: [] } }

// The shell's stream dropped. The rows stay, marked as unavailable, and take no decision.
export const Unavailable: Story = {
  args: Pending.args,
  decorators: [
    (Story) => (
      <ActionSignalContext value={{ counts: null, connected: false }}>
        <Story />
      </ActionSignalContext>
    ),
  ],
}
