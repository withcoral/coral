import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, within } from 'storybook/test'
import { CodeBlock } from './code-block'

const meta = {
  title: 'Wax/CodeBlock',
  component: CodeBlock,
  args: { value: '{"assignee":"me","limit":50,"fields":["id","title","state"]}', format: 'json' },
} satisfies Meta<typeof CodeBlock>
export default meta
type Story = StoryObj<typeof meta>
export const Json: Story = {
  play: async ({ canvasElement }) => {
    await expect(within(canvasElement).getByText(/"assignee"/).textContent).toBe(`{
  "assignee": "me",
  "limit": 50,
  "fields": ["id", "title", "state"]
}`)
  },
}
export const ExactNumbersAndEmbeddedText: Story = {
  args: {
    value: String.raw`{"id":9007199254740993,"content":[{"type":"text","text":"{\"id\":9007199254740993}"}]}`,
  },
}
export const Source: Story = {
  args: {
    format: 'text',
    value: 'const result = await coral.providers.billing.listInvoices({});\nreturn result;',
  },
}
export const TypeScript: Story = {
  args: {
    format: 'typescript',
    framed: true,
    value: `type Input = { days?: number; assignee?: string };

const DAY_MS = 24 * 60 * 60 * 1000;

/** Open issues nobody has updated in \`days\` days, oldest first. */
export default async function staleIssues(input: Input = {}): Promise<Issue[]> {
  const cutoff = new Date(Date.now() - (input.days ?? 14) * DAY_MS);
  const { data } = await coral.providers.linear.listIssues({ assignee: input.assignee ?? "me" });
  return data.issues.filter((issue: Issue) => new Date(issue.updatedAt) < cutoff);
}`,
  },
}
/** A long value stays open, grows to `maxLines`, then scrolls inside the block. */
export const Capped: Story = {
  args: {
    format: 'typescript',
    framed: true,
    maxLines: 8,
    value: Array.from(
      { length: 40 },
      (_, index) =>
        `type Field${index} = { "id": number; "name": string; "created_at": string | null; "labels": Array<string>; "url": string };`,
    ).join('\n'),
  },
}
export const FramedWrapped: Story = {
  args: {
    format: 'typescript',
    framed: true,
    wrap: true,
    value:
      'merge(input: { body?: PutReposOwnerRepoPullsPull_numberMergeBodyInput; owner: string; pull_number: number; repo: string }): Promise<{ data: PullRequestMergeResult }>',
  },
}
/** A capped block keeps `wrap`: one long line wraps instead of scrolling sideways. */
export const CappedWrapped: Story = {
  args: { ...FramedWrapped.args, maxLines: 8 },
  play: async ({ canvasElement }) => {
    const block = canvasElement.querySelector('pre')!
    await expect(getComputedStyle(block).whiteSpace).toBe('pre-wrap')
    await expect(block.scrollWidth).toBeLessThanOrEqual(block.clientWidth)
  },
}
export const Empty: Story = { args: { value: '{}' } }
export const Framed: Story = {
  args: { format: 'text', framed: true, value: 'dd-api-key: ‹API key›\nx-api-version: 2024-01' },
}
/** The copy button sits in the frame beside the code, which scrolls sideways without running under it. */
export const Copyable: Story = {
  args: {
    format: 'text',
    value:
      'claude mcp add --transport http coral https://coral.example.com/workspaces/demo/mcp --header "Authorization: Bearer ••••••••••••"',
    copy: { text: 'storybook-only-command', label: 'Copy command' },
  },
  play: async ({ canvasElement }) => {
    await expect(within(canvasElement).getByRole('button', { name: 'Copy command' })).toBeVisible()
  },
}
