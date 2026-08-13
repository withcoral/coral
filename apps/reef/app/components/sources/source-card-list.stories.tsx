import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import type { CatalogEntry } from '@/lib/sources'

import { SourceCardList } from './source-card-list'

const entries: CatalogEntry[] = [
  {
    description: 'Sync issues, pull requests, and code from your repositories.',
    installed: true,
    name: 'github',
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
    description: 'Imported source',
    installed: false,
    name: 'custom_warehouse',
    origin: 'imported',
    version: '0.1.0',
  },
  {
    description: 'Query projects, branches, databases, and endpoints from Neon.',
    installed: false,
    name: 'neon',
    origin: 'preset',
    preset: { specUrl: 'https://neon.tech/api_spec/release/v2.json', surfaceType: 'openapi' },
    version: '',
  },
]

const meta = {
  args: {
    entries,
    onPick: fn(),
  },
  component: SourceCardList,
  parameters: {
    layout: 'padded',
  },
  render: (args) => (
    <div style={{ maxWidth: 960 }}>
      <SourceCardList {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/Sources/SourceCardList',
} satisfies Meta<typeof SourceCardList>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
