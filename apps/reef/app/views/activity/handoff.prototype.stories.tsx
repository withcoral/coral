// PROTOTYPE — throwaway stories for handoff.prototype.tsx (withcoral/coral#2739).
// Open a story, then flip variants with the floating bar or ← →.
import { createContext, useContext } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { HandoffPrototype, type HandoffPage } from './handoff.prototype'
import { freezeStoryClock, minutesAfterStoryNow, storyNow } from './story-clock'

const secondsAgo = (seconds: number) => new Date(storyNow - seconds * 1000).toISOString()

/** The scenario: in Claude Code you asked for the overdue invoice reminders. The assistant
 * said: "Coral needs your approval for run quiet-harbor-lantern-moss. Open <link> within
 * 13 minutes." You opened the link. */
const program = [
  'const invoices = await coral.providers.billing.listOverdue({ limit: 10 })',
  'for (const invoice of invoices) {',
  '  await coral.providers.mail.send({',
  '    to: invoice.owner,',
  '    subject: `Invoice ${invoice.id} is overdue`,',
  "    body: 'Your invoice is ready for review.',",
  '  })',
  '  await coral.providers.billing.updateInvoice({ invoice: invoice.id, reminderSent: true })',
  '}',
  'return { reminded: invoices.length }',
].join('\n')

const run: HandoffPage['run'] = {
  id: 'quiet-harbor-lantern-moss',
  intent: 'Send the overdue invoice reminders',
  client: 'Claude Code',
  workspaceLabel: 'Finance',
  acceptedAt: secondsAgo(2 * 60 + 10),
  deadlineAt: minutesAfterStoryNow(12),
  state: 'running',
  program,
}

const mailApproval = {
  kind: 'approval' as const,
  name: 'workspaces/finance/approvals/mail',
  callPath: 'coral.providers.mail.send',
  providerLabel: 'Mail',
  arguments:
    '{"to":"owner@example.com","subject":"Invoice INV-4171 is overdue","body":"Your invoice is ready for review."}',
  askedAt: secondsAgo(40),
  line: 3,
}
const updateApproval = {
  kind: 'approval' as const,
  name: 'workspaces/finance/approvals/update',
  callPath: 'coral.providers.billing.updateInvoice',
  providerLabel: 'Billing',
  arguments: '{"invoice":"INV-4171","reminderSent":true}',
  askedAt: secondsAgo(40),
  line: 8,
}

const oneApproval: HandoffPage = {
  run,
  requests: [mailApproval],
  linked: mailApproval.name,
  owner: false,
}

const PageContext = createContext<HandoffPage>(oneApproval)
function Mounted() {
  const page = useContext(PageContext)
  return (
    <PageLayout
      variant="wide"
      breadcrumbs={trail([{ label: 'Activity', to: '/workspaces/finance/activity' }], page.run.id)}
    >
      <HandoffPrototype page={page} />
    </PageLayout>
  )
}
const Stub = createRoutesStub([{ path: '*', Component: Mounted }])

function Story({ page, entry }: { page: HandoffPage; entry: string }) {
  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
      <PageContext value={page}>
        <Stub key={entry} initialEntries={[entry]} />
      </PageContext>
    </div>
  )
}

const meta = {
  title: 'Activity/PROTOTYPE handoff page',
  component: Story,
  beforeEach: freezeStoryClock,
  parameters: { layout: 'fullscreen', chromatic: { disableSnapshot: true } },
  args: {
    page: oneApproval,
    entry: `/workspaces/finance/runs/${run.id}?request=${mailApproval.name}&variant=A`,
  },
} satisfies Meta<typeof Story>
export default meta
type S = StoryObj<typeof meta>

/** The link opened on the one approval the assistant named. */
export const OneApproval: S = {}

/** The Program stopped twice before you arrived: "Approve all 2" is on the table. */
export const TwoApprovals: S = {
  args: {
    page: { ...oneApproval, requests: [mailApproval, updateApproval] },
  },
}

/** You own the Workspace: the shortcut to stop asking for the tool shows. */
export const OwnerSeesPolicyShortcut: S = {
  args: {
    page: {
      ...oneApproval,
      requests: [mailApproval, updateApproval],
      owner: true,
    },
  },
}

/** The Run needs your Linear account; the request outlives the Run Deadline. */
export const AccountConnection: S = {
  args: {
    page: {
      run: {
        ...run,
        id: 'amber-finch-quiet-ledger',
        intent: 'Read my open Linear issues',
        program: "return await coral.providers.linear.listIssues({ assignee: 'me' })",
      },
      requests: [
        {
          kind: 'account_connection',
          name: 'workspaces/finance/accountConnections/linear',
          callPath: 'coral.providers.linear.listIssues',
          providerLabel: 'Linear',
          askedAt: secondsAgo(25),
          expiresAt: minutesAfterStoryNow(14),
          line: 1,
        },
      ],
      linked: 'workspaces/finance/accountConnections/linear',
      owner: false,
    },
    entry: `/workspaces/finance/runs/amber-finch-quiet-ledger?request=workspaces/finance/accountConnections/linear&variant=A`,
  },
}

/** You opened the link after the Run Deadline passed: the words the page shows then. */
export const RunEnded: S = {
  args: {
    page: {
      ...oneApproval,
      run: { ...run, state: 'timed_out', deadlineAt: secondsAgo(3 * 60) },
    },
  },
}

/** No `?request=`: you came from the sidebar, not the assistant's link. */
export const NoLinkedRequest: S = {
  args: {
    page: {
      ...oneApproval,
      requests: [mailApproval, updateApproval],
      linked: null,
    },
    entry: `/workspaces/finance/runs/${run.id}?variant=A`,
  },
}
