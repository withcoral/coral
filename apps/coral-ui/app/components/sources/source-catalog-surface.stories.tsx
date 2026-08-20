import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'

import { expect, fn } from 'storybook/test'

import type { CatalogEntry } from '@/lib/sources'
import { Button } from '@/wax/components'
import { theme } from '@/wax/theme/theme.css'

import { SourceCatalogSurface } from './source-catalog-surface'

const entries: CatalogEntry[] = [
  {
    description: 'Sync issues, pull requests, and code from your repositories.',
    installed: true,
    name: 'github',
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
    description: 'Imported source',
    installed: false,
    name: 'custom_warehouse',
    origin: 'imported',
    version: '0.1.0',
  },
]

const meta = {
  component: SourceCatalogSurface,
  parameters: {
    layout: 'fullscreen',
  },
  render: (args) => <StatefulSurface {...args} />,
  tags: ['autodocs'],
  title: 'Components/Sources/SourceCatalogSurface',
} satisfies Meta<typeof SourceCatalogSurface>

export default meta
type Story = StoryObj<typeof meta>

export const Full: Story = {
  args: {
    entries,
    loadState: 'idle',
    onPick: fn(),
    onSearchChange: fn(),
    search: '',
  },
}

export const WithHeaderAction: Story = {
  args: {
    entries,
    headerAction: (
      <Button.Container size="36" variant="primary">
        <Button.Icon name="Plus" />
        <Button.Text>Create source</Button.Text>
      </Button.Container>
    ),
    loadState: 'idle',
    onPick: fn(),
    onSearchChange: fn(),
    search: '',
  },
  play: async ({ canvas }) => {
    const search = canvas.getByPlaceholderText('Search sources…')
    const action = canvas.getByRole('button', { name: 'Create source' })

    expect(search.getBoundingClientRect().width).toBeLessThanOrEqual(280)
    expect(
      Math.abs(action.getBoundingClientRect().height - search.getBoundingClientRect().height),
    ).toBeLessThanOrEqual(1)
  },
}

export const Compact: Story = {
  args: {
    entries,
    loadState: 'idle',
    onPick: fn(),
    onSearchChange: fn(),
    search: '',
    variant: 'compact',
  },
  render: (args) => (
    <StoryShell>
      <StatefulSurface {...args} />
    </StoryShell>
  ),
}

export const Loading: Story = {
  args: {
    entries: [],
    loadState: 'loading',
    onPick: fn(),
    onSearchChange: fn(),
    search: '',
  },
}

export const Error: Story = {
  args: {
    entries: [],
    errorMessage: 'Unable to reach the Coral API.',
    loadState: 'error',
    onPick: fn(),
    onRetry: fn(),
    onSearchChange: fn(),
    search: '',
  },
}

export const EmptyCatalog: Story = {
  args: {
    entries: [],
    loadState: 'idle',
    onPick: fn(),
    onSearchChange: fn(),
    search: '',
  },
}

export const NoSearchResults: Story = {
  args: {
    entries,
    loadState: 'idle',
    onPick: fn(),
    onSearchChange: fn(),
    search: 'salesforce',
  },
}

function StatefulSurface(args: React.ComponentProps<typeof SourceCatalogSurface>) {
  const [search, setSearch] = useState(args.search)

  return (
    <SourceCatalogSurface
      {...args}
      search={search}
      onSearchChange={(nextSearch) => {
        setSearch(nextSearch)
        args.onSearchChange(nextSearch)
      }}
    />
  )
}

function StoryShell({ children }: { children: React.ReactNode }) {
  return (
    <main
      style={{
        alignItems: 'center',
        background: theme.surface.mainContent,
        boxSizing: 'border-box',
        display: 'flex',
        minHeight: '100dvh',
        padding: 32,
      }}
    >
      <div
        style={{
          border: `1px solid ${theme.stroke.secondary}`,
          height: 620,
          maxWidth: 720,
          overflow: 'hidden',
          width: '100%',
        }}
      >
        {children}
      </div>
    </main>
  )
}
