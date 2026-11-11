import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, within } from 'storybook/test'

import { DiffView } from './diff-view'

const before = `type Input = { days?: number };

const DAY_MS = 24 * 60 * 60 * 1000;

/** Open issues nobody has updated in \`days\` days. */
export default async function staleIssues(input: Input = {}): Promise<Issue[]> {
  const cutoff = new Date(Date.now() - (input.days ?? 14) * DAY_MS);
  const { data } = await coral.providers.linear.listIssues({ assignee: "me" });
  return data.issues.filter((issue: Issue) => new Date(issue.updatedAt) < cutoff);
}

type Issue = { id: string; title: string; updatedAt: string };
`
const after = before
  .replace('{ days?: number }', '{ days?: number; assignee?: string }')
  .replace('days. */', 'days, oldest first. */')
  .replace('assignee: "me"', 'assignee: input.assignee ?? "me"')

const meta = {
  title: 'Wax/DiffView',
  component: DiffView,
  args: { before, after, label: 'Source changes' },
} satisfies Meta<typeof DiffView>
export default meta
type Story = StoryObj<typeof meta>

/** Each change with its context; the unchanged lines between and after them fold. */
export const Changes: Story = {
  args: { context: 1 },
  play: async ({ canvasElement }) => {
    const diff = within(canvasElement).getByRole('group', { name: 'Source changes' })
    // A sign and a spoken word mark each change, so its colour is not the only cue.
    await expect(within(diff).getAllByText('Removed:')).toHaveLength(3)
    await expect(within(diff).getAllByText('Added:')).toHaveLength(3)
    await expect(within(diff).getAllByText(/unchanged lines?$/).length).toBeGreaterThan(0)
  },
}
export const Unchanged: Story = {
  args: { after: before, unchanged: 'The source does not change.' },
}
/** A long diff grows to `maxLines`, then scrolls inside the frame. */
export const Capped: Story = { args: { maxLines: 6 } }
export const EverythingAdded: Story = { args: { before: '' } }
/** A long line scrolls sideways, and its tint reaches the end of the line. */
export const LongLine: Story = {
  args: {
    before: 'const a = 1',
    after: `const a = ${JSON.stringify('a very long string '.repeat(20))}`,
  },
}
/** Two texts whose lines are the same but whose line breaks differ do not read as unchanged. */
export const LineBreaksOnly: Story = {
  args: { after: before.replaceAll('\n', '\r\n') },
  play: async ({ canvasElement }) => {
    await expect(
      within(canvasElement).getByText('Only the line breaks change.'),
    ).toBeInTheDocument()
  },
}
