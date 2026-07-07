import type { Meta, StoryObj } from '@storybook/react-vite'

import { Link, MemoryRouter } from 'react-router'
import { fn } from 'storybook/test'

import { Icon } from '@/wax/components/icon'

import { CardList, type CardItem, type CardListProps } from './card-list'

const baseItems: CardItem[] = [
  {
    description: 'Monitor logs, metrics, and traces across your services.',
    icon: <Icon name="Activity" size="20" />,
    id: 'datadog',
    title: 'Datadog',
  },
  {
    description: 'Sync issues, pull requests, and code from your repositories.',
    icon: <Icon name="GitBranch" size="20" />,
    id: 'github',
    title: 'GitHub',
  },
  {
    description: 'Query your application and analytics data.',
    icon: <Icon name="Database" size="20" />,
    id: 'postgres',
    title: 'Postgres',
  },
  {
    description: 'Search and reference your team knowledge base.',
    icon: <Icon name="BookOpen" size="20" />,
    id: 'notion',
    title: 'Notion',
  },
]

// Generate `count` items by cycling the mock data, with unique ids/labels.
function makeItems(count: number): CardItem[] {
  return Array.from({ length: count }, (_, index) => {
    const base = baseItems[index % baseItems.length]
    return { ...base, id: `${base.id}-${index}`, title: `${base.title} ${index + 1}` }
  })
}

// `count` is a story-only control that drives how many cards CardList renders;
// `items` is generated from it in render, so its own control is disabled.
type StoryArgs = CardListProps & { count: number }

const meta = {
  args: {
    count: baseItems.length,
    items: baseItems,
  },
  argTypes: {
    count: {
      control: { max: 100, min: 0, step: 1, type: 'range' },
    },
    items: {
      control: false,
    },
  },
  component: CardList,
  parameters: {
    layout: 'padded',
  },
  render: ({ count, ...args }) => <CardList {...args} items={makeItems(count)} />,
  tags: ['autodocs'],
  title: 'Wax/CardList',
} satisfies Meta<StoryArgs>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Interactive: Story = {
  args: {
    onSelect: fn(),
  },
}

export const AsReactRouterLinks: Story = {
  render: ({ count }) => {
    const items = makeItems(count)
    return (
      <MemoryRouter>
        <CardList
          as={Link}
          getCardProps={(item) => ({
            prefetch: 'intent' as const,
            to: `/sources/${item.id}`,
          })}
          items={items}
        />
      </MemoryRouter>
    )
  },
}
