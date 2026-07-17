import type { Meta, StoryObj } from '@storybook/react-vite'

import { Link, MemoryRouter } from 'react-router'
import { fn } from 'storybook/test'

import { Card } from '@/wax/components'
import { Icon } from '@/wax/components/icon'

type StoryCard = Pick<Card.CardProps, 'description' | 'headerPill' | 'icon' | 'title'> & {
  id: string
}

const baseCards: StoryCard[] = [
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

function makeCards(count: number): StoryCard[] {
  return Array.from({ length: count }, (_, index) => {
    const base = baseCards[index % baseCards.length]
    return { ...base, id: `${base.id}-${index}`, title: `${base.title} ${index + 1}` }
  })
}

type StoryArgs = React.ComponentProps<typeof Card.List> & {
  count: number
  onSelect?: (id: string) => void
}

const meta = {
  args: {
    count: baseCards.length,
  },
  argTypes: {
    count: {
      control: { max: 100, min: 0, step: 1, type: 'range' },
    },
  },
  component: Card.List,
  parameters: {
    layout: 'padded',
  },
  render: ({ count, onSelect, ...args }) => (
    <Card.List {...args}>
      {makeCards(count).map(({ id, ...card }) => (
        <Card.Item key={id}>
          {onSelect ? (
            <Card.Card {...card} as="button" onClick={() => onSelect(id)} />
          ) : (
            <Card.Card {...card} />
          )}
        </Card.Item>
      ))}
    </Card.List>
  ),
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
    const cards = makeCards(count)
    return (
      <MemoryRouter>
        <Card.List>
          {cards.map(({ id, ...card }) => (
            <Card.Item key={id}>
              <Card.Card {...card} as={Link} prefetch="intent" to={`/sources/${id}`} />
            </Card.Item>
          ))}
        </Card.List>
      </MemoryRouter>
    )
  },
}
