import { useEffect, useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { MemoryRouter } from 'react-router'

import { fn } from 'storybook/test'

import type { CatalogEntry } from '@/lib/sources'

import { OnboardingSourcesPage } from './onboarding-sources-page'
import { getOnboardingStepState } from './onboarding-steps'

const entries: CatalogEntry[] = [
  {
    description: 'Sync issues, pull requests, and code from your repositories.',
    installed: true,
    name: 'github',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Sync merge requests, pipelines, issues, and repository metadata.',
    installed: false,
    name: 'gitlab',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Inspect errors, releases, issues, and project health.',
    installed: false,
    name: 'sentry',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Read pages, databases, comments, and workspace content.',
    installed: false,
    name: 'notion',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Query messages and metadata from Gmail.',
    installed: false,
    name: 'gmail',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Browse project issues, labels, milestones, and users.',
    installed: false,
    name: 'linear',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Query incidents, services, and schedules from PagerDuty.',
    installed: false,
    name: 'pagerduty',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Analyze feature flags, environments, and flag targeting.',
    installed: false,
    name: 'launchdarkly',
    origin: 'bundled',
    version: '1.0.0',
  },
]

const meta = {
  args: {
    entries,
    loadState: 'idle',
    onRetry: fn(),
    onSearchChange: fn(),
    onSourceSelect: fn(),
    search: '',
    step: getOnboardingStepState('sources'),
  },
  component: OnboardingSourcesPage,
  parameters: {
    layout: 'fullscreen',
  },
  render: (args) => (
    <MemoryRouter>
      <StatefulPage {...args} />
    </MemoryRouter>
  ),
  tags: ['autodocs'],
  title: 'Components/Onboarding/SourcesPage',
} satisfies Meta<typeof OnboardingSourcesPage>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

function StatefulPage(args: React.ComponentProps<typeof OnboardingSourcesPage>) {
  const [search, setSearch] = useState(args.search)

  useEffect(() => {
    setSearch(args.search)
  }, [args.search])

  return (
    <OnboardingSourcesPage
      {...args}
      search={search}
      onSearchChange={(nextSearch) => {
        setSearch(nextSearch)
        args.onSearchChange(nextSearch)
      }}
    />
  )
}
