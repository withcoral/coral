import { useCallback, useEffect, useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import type { CatalogEntry } from '@/lib/sources'

import {
  OnboardingSampleQueryPage,
  type OnboardingSampleQueryRow,
} from './onboarding-sample-query-page'

const connectedSources: CatalogEntry[] = [
  connectedSource('clickup', 'Query tasks, lists, and workspace activity from ClickUp.'),
  connectedSource('gitlab', 'Query projects, merge requests, pipelines, and more from GitLab.'),
  connectedSource('linear', 'Query issues, projects, and teams from Linear.'),
  connectedSource('notion', 'Query pages, databases, and users from Notion.'),
  connectedSource('slack', 'Query messages and metadata from Slack.'),
]

const rows: OnboardingSampleQueryRow[] = [
  { source: 'clickup', tables: 46 },
  { source: 'gitlab', tables: 216 },
  { source: 'linear', tables: 10 },
  { source: 'notion', tables: 12 },
  { source: 'slack', tables: 2 },
]

function connectedSource(name: string, description: string): CatalogEntry {
  return {
    description,
    installed: true,
    name,
    origin: 'bundled',
    version: '1.0.0',
  }
}

const meta = {
  args: {
    connectedSources,
    onContinue: fn(),
    onRunSampleQuery: fn(),
    rows,
  },
  component: OnboardingSampleQueryPage,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  title: 'Components/Onboarding/SampleQueryPage',
} satisfies Meta<typeof OnboardingSampleQueryPage>

export default meta
type Story = StoryObj<typeof meta>

export const QuerySucceeded: Story = {
  args: {
    loadState: 'success',
  },
}

export const SingleSourceReady: Story = {
  args: {
    connectedSources: [connectedSource('slack', 'Query messages and metadata from Slack.')],
    loadState: 'success',
    rows: [{ source: 'slack', tables: 2 }],
  },
}

export const QueryRunning: Story = {
  args: {
    loadState: 'loading',
    rows: [],
  },
}

export const QueryFailed: Story = {
  args: {
    errorMessage: 'Coral could not load its source catalog.',
    loadState: 'error',
    rows: [],
  },
}

export const InteractiveRun: Story = {
  args: {
    loadState: 'idle',
    rows: [],
  },
  render: (args) => <InteractiveSampleQueryPage {...args} />,
}

function InteractiveSampleQueryPage(args: React.ComponentProps<typeof OnboardingSampleQueryPage>) {
  const [loadState, setLoadState] = useState(args.loadState ?? 'idle')
  const [queryRows, setQueryRows] = useState(args.rows ?? [])

  useEffect(() => {
    setLoadState(args.loadState ?? 'idle')
    setQueryRows(args.rows ?? [])
  }, [args.loadState, args.rows])

  const handleRunSampleQuery = useCallback(
    (sql: string) => {
      args.onRunSampleQuery?.(sql)
      setLoadState('loading')
      window.setTimeout(() => {
        setQueryRows(rows)
        setLoadState('success')
      }, 700)
    },
    [args],
  )

  return (
    <OnboardingSampleQueryPage
      {...args}
      loadState={loadState}
      onRunSampleQuery={handleRunSampleQuery}
      rows={queryRows}
    />
  )
}
