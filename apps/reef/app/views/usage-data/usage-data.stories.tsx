import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { createRoutesStub } from 'react-router'
import { expect, userEvent, within } from 'storybook/test'
import type { UsageDataView } from './types'
import { UsageDataPage } from './usage-data-page'

/**
 * A fixture shape for one daily report. Coral owns the real one and hands the page its exact JSON: a random install ID, dates, counts and
 * values from fixed lists. Each report is a full snapshot. The collector keeps the first date of
 * each milestone, the latest totals, and adds up the days.
 */
interface UsageReport {
  v: 1
  install: string
  coral: {
    version: string
    os: 'macos' | 'linux' | 'windows'
    arch: 'arm64' | 'x86_64'
    kind: 'desktop' | 'server'
    /** `disabled` is one local user; `required` is sign-in for many. */
    auth: 'disabled' | 'required'
    database: 'sqlite' | 'postgres'
  }
  build: 'release' | 'source'
  /** When each first happened. A date never changes once it is set. */
  milestones: {
    first_successful_run: string | null
    first_saved_function: string | null
    function_reused_in_later_task: string | null
  }
  /** What happened on one UTC day. The next report starts again from zero. */
  day: {
    date: string
    runs: { succeeded: number; failed: number; timed_out: number; canceled: number }
    /** Coral's own failure kinds, never an error message. */
    run_failures: Partial<Record<RunFailure, number>>
    tool_invocations: { succeeded: number; failed: number; approval_required: number }
    approvals: { approved: number; denied: number; expired: number }
    saved_functions: { saved: number; called: number; tasks_reusing_one: number }
    tasks: { started: number }
    /** Runs by MCP client, mapped to a fixed list. Any other client is `other`. */
    clients: Partial<Record<McpClient, number>>
  }
  /** How much exists now. A total can go down. */
  totals: {
    workspaces: number
    users: number
    agents: number
    saved_functions: number
    /** Providers made from a Coral recipe, by recipe. Custom providers are only counted. */
    providers: {
      recipe: Record<string, number>
      custom_openapi: number
      custom_mcp: number
    }
    accounts: { oauth: number; secret: number }
  }
}

type RunFailure = 'compile' | 'tool_error' | 'timeout' | 'cpu_budget' | 'interrupted'
type McpClient = 'claude-code' | 'claude-desktop' | 'cursor' | 'codex' | 'chatgpt' | 'other'

// Fixtures. The install ID is random per install. The next report covers today so far.
const report: UsageReport = {
  v: 1,
  install: '3f9c8d2e-5b41-4c7a-9e06-1d7b2c4fa21e',
  coral: {
    version: '0.14.2',
    os: 'macos',
    arch: 'arm64',
    kind: 'desktop',
    auth: 'disabled',
    database: 'sqlite',
  },
  build: 'release',
  milestones: {
    first_successful_run: '2026-11-04',
    first_saved_function: '2026-11-05',
    function_reused_in_later_task: '2026-11-06',
  },
  day: {
    date: '2026-11-06',
    runs: { succeeded: 41, failed: 6, timed_out: 1, canceled: 0 },
    run_failures: { compile: 3, tool_error: 2, timeout: 1, cpu_budget: 1 },
    tool_invocations: { succeeded: 212, failed: 9, approval_required: 4 },
    approvals: { approved: 3, denied: 1, expired: 0 },
    saved_functions: { saved: 2, called: 17, tasks_reusing_one: 3 },
    tasks: { started: 9 },
    clients: { 'claude-code': 30, cursor: 11, other: 7 },
  },
  totals: {
    workspaces: 2,
    users: 1,
    agents: 1,
    saved_functions: 5,
    providers: { recipe: { github: 1, linear: 1, gmail: 1 }, custom_openapi: 1, custom_mcp: 0 },
    accounts: { oauth: 3, secret: 1 },
  },
}
// Yesterday's report: the reuse milestone had not happened yet, and there was less set up.
const earlier: UsageReport = {
  ...report,
  milestones: { ...report.milestones, function_reused_in_later_task: null },
  day: {
    date: '2026-11-05',
    runs: { succeeded: 12, failed: 4, timed_out: 0, canceled: 1 },
    run_failures: { compile: 3, tool_error: 1 },
    tool_invocations: { succeeded: 58, failed: 5, approval_required: 2 },
    approvals: { approved: 2, denied: 0, expired: 0 },
    saved_functions: { saved: 3, called: 4, tasks_reusing_one: 0 },
    tasks: { started: 5 },
    clients: { 'claude-code': 17 },
  },
  totals: {
    ...report.totals,
    saved_functions: 3,
    providers: { recipe: { github: 1, linear: 1 }, custom_openapi: 1, custom_mcp: 0 },
    accounts: { oauth: 2, secret: 1 },
  },
}
// The same reports from a server: many users sign in, and it keeps its data in Postgres.
const onServer = (r: UsageReport): UsageReport => ({
  ...r,
  coral: {
    ...r.coral,
    os: 'linux',
    arch: 'x86_64',
    kind: 'server',
    auth: 'required',
    database: 'postgres',
  },
})
const sharing: UsageDataView = {
  choice: 'on',
  canChange: true,
  kind: 'desktop',
  next: JSON.stringify(report),
  lastSent: { at: '2026-11-06T00:00:00Z', json: JSON.stringify(earlier) },
}

/** The page with a working switch. A real owner's choice goes to the server. */
function Demo({ view: initial }: { view: UsageDataView }) {
  const [view, setView] = useState(initial)
  return (
    <UsageDataPage
      view={view}
      onChoose={(share) => setView((v) => ({ ...v, choice: share ? 'on' : 'off' }))}
    />
  )
}

const meta = {
  title: 'Reef/Usage data',
  component: Demo,
  parameters: { layout: 'fullscreen' },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: () => <Story /> }])
      return <Stub />
    },
  ],
} satisfies Meta<typeof Demo>
export default meta
type Story = StoryObj<typeof meta>

/** Sharing is on by default. */
export const Sharing: Story = {
  args: { view: sharing },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const toggle = canvas.getByRole('switch', { name: 'Share anonymous usage data' })
    await userEvent.click(toggle)
    await expect(toggle).not.toBeChecked()
    await expect(canvas.getByText('Coral sends no usage data while sharing is off.')).toBeVisible()
  },
}

export const NotSharing: Story = {
  args: { view: { ...sharing, choice: 'off' } },
}

/** A member reads the same reports, but cannot change the setting. */
export const Member: Story = {
  args: { view: { ...sharing, canChange: false } },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(
      canvas.getByRole('switch', { name: 'Share anonymous usage data' }),
    ).toHaveAttribute('aria-disabled', 'true')
    await expect(canvas.getByText(/Only organization owners can change this/)).toBeVisible()
  },
}

/** DO_NOT_TRACK, CORAL_TELEMETRY or CI stops sending from one Coral process, whatever the switch shows. */
export const TurnedOffByDoNotTrack: Story = {
  args: { view: { ...sharing, block: { kind: 'environment', variable: 'DO_NOT_TRACK' } } },
}

/** CORAL_TELEMETRY is off on 0 or false, so the banner says off, not set. */
export const TurnedOffByCoralTelemetry: Story = {
  args: {
    view: {
      ...sharing,
      kind: 'server',
      next: JSON.stringify(onServer(report)),
      lastSent: { ...sharing.lastSent!, json: JSON.stringify(onServer(earlier)) },
      block: { kind: 'environment', variable: 'CORAL_TELEMETRY' },
    },
  },
  play: async ({ canvasElement }) => {
    await expect(
      within(canvasElement).getByText(
        'CORAL_TELEMETRY is off. Coral sends no usage data from this server, even when sharing is on.',
      ),
    ).toBeVisible()
  },
}

export const TurnedOffByCi: Story = {
  args: { view: { ...sharing, block: { kind: 'environment', variable: 'CI' } } },
}

/** Source and dev builds show the report but never send it. */
export const SourceBuild: Story = {
  args: {
    view: {
      ...sharing,
      block: { kind: 'build' },
      next: JSON.stringify({ ...report, build: 'source' }),
      lastSent: undefined,
    },
  },
}
