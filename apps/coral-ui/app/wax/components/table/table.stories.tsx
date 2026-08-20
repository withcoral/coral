import type { Meta, StoryObj } from '@storybook/react-vite'

import { theme } from '@/wax/theme/theme.css'

import { Body } from './body'
import { Cell } from './cell'
import type { Column } from './columns'
import type { TableDensity } from './constants'
import { Container } from './container'
import { Head } from './head'
import { Heading } from './heading'
import { Row } from './row'
import { Status } from './status'

const meta: Meta<typeof Container> = {
  component: Container,
  decorators: [
    (Story) => (
      <div style={{ backgroundColor: theme.surface.mainContent, padding: 24 }}>
        <Story />
      </div>
    ),
  ],
  title: 'Wax/Table',
}

export default meta
type Story = StoryObj<typeof Container>

const PEOPLE_COLUMNS: Column[] = [
  { label: 'Name', width: 'fill' },
  { label: 'Role', width: 'fill' },
  { align: 'right', label: 'Requests', width: 'content' },
]

function ExampleTable({ density }: { density: TableDensity }) {
  return (
    <Container columns={PEOPLE_COLUMNS} density={density}>
      <Head />
      <Body>
        <Row>
          <Cell>Ada Lovelace</Cell>
          <Cell>Admin</Cell>
          <Cell mono>24</Cell>
        </Row>
        <Row>
          <Cell>Grace Hopper</Cell>
          <Cell>User</Cell>
          <Cell mono>12</Cell>
        </Row>
      </Body>
    </Container>
  )
}

export const Compact: Story = {
  render: () => <ExampleTable density="compact" />,
}

export const Default: Story = {
  render: () => <ExampleTable density="default" />,
}

// Both constrained-height arrangements. `maxHeight` keeps the table heading
// outside its row scrollport; an ancestor scroll keeps the heading sticky.
const SCROLL_COLUMNS: Column[] = [
  { label: 'Name', width: 'fill' },
  { align: 'right', label: 'Requests', width: 'content' },
]

function ManyRows() {
  return (
    <Body>
      {Array.from({ length: 24 }, (_, index) => (
        <Row key={index}>
          <Cell mono>row_{index}</Cell>
          <Cell mono>{index * 7}</Cell>
        </Row>
      ))}
    </Body>
  )
}

export const ScrollingRowsInsideTable: Story = {
  render: () => (
    <Container
      columns={SCROLL_COLUMNS}
      density="compact"
      layout="fixed"
      maxHeight={180}
      variant="card"
    >
      <Head />
      <ManyRows />
    </Container>
  ),
}

export const StickyHeadingOnAncestorScroll: Story = {
  render: () => (
    <div style={{ maxHeight: 180, overflow: 'auto' }}>
      <Container columns={SCROLL_COLUMNS} density="compact" layout="fixed" variant="card">
        <Head />
        <ManyRows />
      </Container>
    </div>
  ),
}

const FEATURE_COLUMNS: Column[] = [
  { label: 'Feature', width: 'fill' },
  { align: 'right', ariaLabel: 'Enabled', width: 96 },
]

export const Wrapping: Story = {
  render: () => (
    <Container columns={FEATURE_COLUMNS} layout="fixed">
      <Head />
      <Body>
        <Row>
          <Cell wrap>
            Long-form prose in a cell wraps onto as many lines as it needs, and the row grows with
            it.
          </Cell>
          <Cell mono>on</Cell>
        </Row>
        <Status>A status row stands in for the rows, and takes no hover.</Status>
      </Body>
    </Container>
  ),
}

// A caller renders its own heading row when a label cannot express it.
export const CustomHeadings: Story = {
  render: () => (
    <Container columns={SCROLL_COLUMNS} density="compact">
      <Head>
        <Row>
          <Heading>Name (custom)</Heading>
          <Heading>Requests</Heading>
        </Row>
      </Head>
      <ManyRows />
    </Container>
  ),
}
