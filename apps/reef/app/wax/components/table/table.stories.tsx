import type { Meta, StoryObj } from '@storybook/react-vite'
import { useEffect, useRef, useState } from 'react'
import { expect, userEvent, waitFor, within } from 'storybook/test'
import * as Button from '@/wax/components/button'
import { TextInput } from '@/wax/components/inputs/text/text-input'
import { theme } from '@/wax/theme/theme.css'

import { AddRow } from './add-row'
import { Body } from './body'
import { Cell } from './cell'
import type { Column } from './columns'
import type { TableDensity, TableGroupHeadings } from './constants'
import { Container } from './container'
import { Head } from './head'
import { LoadMore } from './load-more'
import { Group, GroupToggle } from './group'
import { Heading } from './heading'
import { Row } from './row'
import { Status } from './status'
import { CELL_HEIGHT_PX, GROUP_HEADING_HEIGHT_PX } from './constants'
import { useVirtualRows, VirtualRows } from './virtual'
import { Window as ReadingWindow, type WindowViewport } from './window'

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
type NestedGroupsStory = StoryObj<{ depth: number }>

const PEOPLE_COLUMNS: Column[] = [
  { label: 'Name', width: 'fill' },
  { label: 'Role', width: 'fill' },
  { align: 'right', label: 'Requests', width: 'content' },
]

const PROVIDERS = ['Linear', 'GitHub', 'Slack', 'Google Drive']
const OPERATION_GROUPS = ['Issues', 'Projects', 'Comments', 'Search']
const OPERATIONS = ['list', 'get', 'search', 'create', 'update', 'archive', 'assign', 'watch']

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

export const Grouped: Story = {
  render: () => (
    <Container columns={PEOPLE_COLUMNS} layout="fixed">
      <Head />
      {PROVIDERS.map((provider, providerIndex) => (
        <Group key={provider} label={`${provider} provider`}>
          {OPERATIONS.map((operation, operationIndex) => (
            <Row key={operation}>
              <Cell>
                {provider.toLowerCase()}.{operation}
              </Cell>
              <Cell>Operation</Cell>
              <Cell mono>{providerIndex * 16 + operationIndex * 3}</Cell>
            </Row>
          ))}
        </Group>
      ))}
    </Container>
  ),
}

export const Compact: Story = {
  render: () => <ExampleTable density="compact" />,
}

export const Comfortable: Story = {
  render: () => <ExampleTable density="comfortable" />,
}

export const Default: Story = {
  render: () => <ExampleTable density="default" />,
}

export const RowStatusMarkers: Story = {
  render: () => (
    <Container
      columns={[
        { label: 'Record', width: 'fill' },
        { label: 'Duration', width: 88 },
      ]}
      density="compact"
    >
      <Head />
      <Body>
        <Row status={{ tone: 'success', label: 'succeeded' }}>
          <Cell>Completed execution</Cell>
          <Cell mono>24ms</Cell>
        </Row>
        <Row selected status={{ tone: 'error', label: 'failed' }}>
          <Cell>Selected failed execution</Cell>
          <Cell mono>16ms</Cell>
        </Row>
        <Row status={{ tone: 'warning', label: 'indeterminate' }}>
          <Cell>Uncertain dispatch</Cell>
          <Cell mono>2s</Cell>
        </Row>
        <Row status={{ tone: 'neutral', label: 'upstream outcome' }}>
          <Cell mono>coral.providers.linear.listIssues</Cell>
          <Cell mono>140ms</Cell>
        </Row>
      </Body>
    </Container>
  ),
}

// Both constrained-height arrangements. `maxRows` keeps the table heading
// outside its row scrollport; an ancestor scroll keeps the heading sticky.
const SCROLL_COLUMNS: Column[] = [
  { label: 'Name', width: 'fill' },
  { align: 'right', label: 'Requests', width: 'content' },
]

function ManyRows({ label = 'row', start = 0 }: { label?: string; start?: number }) {
  return (
    <Body>
      {Array.from({ length: 24 }, (_, index) => (
        <Row key={index}>
          <Cell mono>
            {label}_{index + start}
          </Cell>
          <Cell mono>{(index + start) * 7}</Cell>
        </Row>
      ))}
    </Body>
  )
}

export const ScrollingRowsInsideTable: Story = {
  render: () => (
    <Container columns={SCROLL_COLUMNS} density="compact" layout="fixed" maxRows={5} variant="card">
      <Head />
      <ManyRows />
    </Container>
  ),
  // Catches: a row cap that cuts the last row or shows part of the next one.
  play: async ({ canvas }) => {
    const port = canvas.getByRole('cell', { name: 'row_0' }).closest('[role=rowgroup]')!
    const bottom = port.getBoundingClientRect().bottom
    const top = (name: string) => canvas.getByRole('cell', { name }).getBoundingClientRect().top
    await expect(top('row_4') + CELL_HEIGHT_PX.compact).toBeCloseTo(bottom, 0)
    await expect(top('row_5')).toBeGreaterThanOrEqual(bottom - 1)
  },
}

function InspectorTable({ groupHeadings }: { groupHeadings?: TableGroupHeadings }) {
  return (
    <div style={{ height: 300, display: 'flex' }}>
      <Container
        ariaLabel="Records"
        groupHeadings={groupHeadings}
        layout="scroll"
        columns={[
          { label: 'Record', width: 'minmax(340px, 1fr)' },
          { label: 'Requests', width: 120, align: 'right' },
        ]}
      >
        <Head />
        {PROVIDERS.map((provider, providerIndex) => (
          <Group key={provider} label={`${provider} provider`}>
            {OPERATION_GROUPS.map((group, groupIndex) => (
              <Group
                key={group}
                label={`${group} operation group`}
                heading={<Cell fullWidth>{group} operation group</Cell>}
              >
                {OPERATIONS.map((operation, operationIndex) => (
                  <Row key={operation}>
                    <Cell>
                      {provider.toLowerCase()}.{group.toLowerCase()}.{operation}
                    </Cell>
                    <Cell mono>{providerIndex * 100 + groupIndex * 10 + operationIndex}</Cell>
                  </Row>
                ))}
              </Group>
            ))}
          </Group>
        ))}
      </Container>
    </div>
  )
}

export const ScrollableInspector: Story = {
  render: () => <InspectorTable />,
}

// Taller headings, a provider on the deeper tint and its groups on the lighter one.
export const ProminentGroupHeadings: Story = {
  render: () => <InspectorTable groupHeadings="prominent" />,
}

// A table-wide status fills a scroll table below its head, whether it sits in
// the body or straight after the head.
export const ScrollTableStatus: Story = {
  render: () => (
    <div style={{ display: 'flex', gap: 16, height: 300 }}>
      {(['In the body', 'After the head'] as const).map((placement) => (
        <Container
          key={placement}
          ariaLabel={placement}
          layout="scroll"
          columns={[{ label: 'Record', width: 'minmax(240px, 1fr)' }]}
        >
          <Head />
          {placement === 'In the body' ? (
            <Body>
              <Status>No records yet.</Status>
            </Body>
          ) : (
            <Status>No records match.</Status>
          )}
        </Container>
      ))}
    </div>
  ),
  play: async ({ canvasElement }) => {
    for (const name of ['In the body', 'After the head']) {
      const table = within(canvasElement).getByRole('table', { name })
      const head = within(table).getAllByRole('rowgroup')[0].getBoundingClientRect()
      const box = table.getBoundingClientRect()
      const status = within(table)
        .getByText(/^No records/)
        .closest('[role="row"]')!
        .getBoundingClientRect()
      // The 1px border is the only space left under the status.
      await expect(Math.abs(status.top - head.bottom)).toBeLessThan(2)
      await expect(Math.abs(box.bottom - status.bottom)).toBeLessThan(2)
    }
  },
}

const recordCells = (time: string, label: string) => (
  <>
    <Cell mono>{time}</Cell>
    <Cell>{label}</Cell>
  </>
)
const recordRows = (prefix: string, count: number) =>
  Array.from({ length: count }, (_, index) => (
    <Row key={index}>
      {recordCells(`09:${String(index).padStart(2, '0')}`, `${prefix} ${index + 1}`)}
    </Row>
  ))
/**
 * Record headings: a group heading that is itself a record row. It keeps its
 * selection and status marker while pinned and takes no group indent, and a
 * nested record heading pins under it, then slides under it as its rows run out.
 */
export const RecordHeadings: Story = {
  render: () => (
    <div style={{ height: 300, display: 'flex' }}>
      <Container
        ariaLabel="Records"
        density="compact"
        layout="scroll"
        columns={[
          { label: 'Time', width: 96 },
          { label: 'Record', width: 'minmax(240px, 1fr)' },
        ]}
      >
        <Head />
        <Group label="Today">
          <Row>{recordCells('10:00', 'Plain record')}</Row>
          <Group
            label="Parent record"
            heading={recordCells('09:59', 'Parent record')}
            headingRow={{
              recordKey: 'parent',
              selected: true,
              status: { tone: 'success', label: 'succeeded' },
            }}
          >
            {recordRows('Parent child', 8)}
            <Group
              label="Nested record"
              heading={recordCells('09:30', 'Nested record')}
              headingRow={{ recordKey: 'nested', status: { tone: 'error', label: 'failed' } }}
            >
              {recordRows('Nested child', 20)}
            </Group>
            {recordRows('Later child', 20)}
          </Group>
        </Group>
      </Container>
    </div>
  ),
  play: async ({ canvasElement }) => {
    const table = within(canvasElement).getByRole('table', { name: 'Records' })
    const row = (label: string) =>
      within(table)
        .getAllByRole('row')
        .find((candidate) => within(candidate).queryByText(label))!
    const box = (label: string) => row(label).getBoundingClientRect()
    const contentTop = (label: string) =>
      box(label).top - table.getBoundingClientRect().top + table.scrollTop
    // Vitest owns what headingRow forwards; this checks layout only. Both headings
    // carry a status, so pinning here also proves the marker keeps them sticky.
    const parent = row('Parent record')

    // A record heading keeps its row layout: no group indent on its first cell.
    await expect(within(parent).getByText('09:59').getBoundingClientRect().left).toBe(
      within(row('Plain record')).getByText('10:00').getBoundingClientRect().left,
    )

    // The nested group does not pin, so measure its rows before scrolling.
    const nestedStart = contentTop('Nested child 1')
    table.scrollTop = nestedStart + 120
    await waitFor(() => expect(box('Nested record').top).toBe(box('Parent record').bottom))
    await expect(box('Parent record').top).toBe(box('Today').bottom)
    // Rows scroll under a pinned heading, so even a selected one must be opaque.
    await expect(getComputedStyle(parent).backgroundColor).not.toMatch(/\/|rgba|transparent/)

    // As the nested rows run out, its heading slides under the parent heading.
    table.scrollTop += box('Nested child 20').bottom - box('Parent record').bottom - 16
    await waitFor(() => expect(box('Nested record').top).toBeLessThan(box('Parent record').bottom))
    const pinned = box('Parent record')
    await expect(
      parent.contains(document.elementFromPoint(pinned.left + 120, pinned.bottom - 4)),
    ).toBe(true)
  },
}

/**
 * A group heading built from several cells in a fragment. Only its first cell
 * takes the group indent, and a nested group indents it one step further.
 */
export const GroupHeadingCells: Story = {
  render: () => (
    <div style={{ height: 240, display: 'flex' }}>
      <Container
        ariaLabel="Records"
        layout="scroll"
        columns={[
          { label: 'Record', width: 'fill' },
          { label: 'Requests', width: 120, align: 'right' },
        ]}
      >
        <Head />
        <Group
          label="Linear provider"
          heading={
            <>
              <Cell>Linear provider</Cell>
              <Cell mono>24</Cell>
            </>
          }
        >
          <Group
            label="Issues operation group"
            heading={
              <>
                <Cell>Issues operation group</Cell>
                <Cell mono>12</Cell>
              </>
            }
          >
            <Row>
              <Cell>linear.issues.list</Cell>
              <Cell mono>7</Cell>
            </Row>
          </Group>
        </Group>
      </Container>
    </div>
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const inset = (text: string) =>
      parseFloat(
        getComputedStyle(canvas.getByText(text).closest('[role="cell"]')!).paddingInlineStart,
      )
    const provider = inset('Linear provider')
    await expect(provider).toBeGreaterThan(0)
    await expect(inset('24')).toBe(0)
    await expect(inset('Issues operation group')).toBe(2 * provider)
    await expect(inset('12')).toBe(0)
  },
}

/** A heading toggle collapses its group to the heading and expands it again. */
export const CollapsibleGroup: Story = {
  render: function Render() {
    const [collapsed, setCollapsed] = useState(false)
    return (
      <Container ariaLabel="Records" columns={[{ label: 'Record', width: 'fill' }]}>
        <Head />
        <Group
          label="Linear provider"
          collapsed={collapsed}
          heading={
            <Cell>
              <GroupToggle
                collapsed={collapsed}
                label="Linear provider"
                onToggle={() => setCollapsed(!collapsed)}
              />
              Linear provider
            </Cell>
          }
        >
          <Row>
            <Cell>linear.issues.list</Cell>
          </Row>
        </Group>
      </Container>
    )
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await userEvent.click(canvas.getByRole('button', { name: 'Collapse Linear provider' }))
    await expect(canvas.queryByText('linear.issues.list')).toBeNull()
    const expand = canvas.getByRole('button', { name: 'Expand Linear provider' })
    await expect(expand).toHaveAttribute('aria-expanded', 'false')
    await userEvent.click(expand)
    await expect(canvas.getByText('linear.issues.list')).toBeVisible()
  },
}

/**
 * The table reports which provider group holds the pinned heading; the label
 * above the table follows it. The empty provider shows a group-scoped status.
 */
export const PinnedGroupReadout: Story = {
  render: function Render() {
    const [pinned, setPinned] = useState<string | null>(null)
    return (
      <div style={{ height: 300, display: 'flex', flexDirection: 'column', gap: 8 }}>
        <div data-testid="pinned">Pinned: {pinned ?? 'none'}</div>
        <Container
          ariaLabel="Records"
          layout="scroll"
          columns={[
            { label: 'Record', width: 'minmax(340px, 1fr)' },
            { label: 'Requests', width: 120, align: 'right' },
          ]}
          onPinnedGroupChange={setPinned}
        >
          <Head />
          {PROVIDERS.map((provider, providerIndex) => (
            <Group id={`provider-${provider}`} key={provider} label={`${provider} provider`}>
              {providerIndex === 1 ? (
                <Status scope="group">No matching operations.</Status>
              ) : (
                OPERATIONS.map((operation, operationIndex) => (
                  <Row key={operation}>
                    <Cell>
                      {provider.toLowerCase()}.{operation}
                    </Cell>
                    <Cell mono>{providerIndex * 100 + operationIndex}</Cell>
                  </Row>
                ))
              )}
            </Group>
          ))}
        </Container>
      </div>
    )
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getByTestId('pinned')).toHaveTextContent('Pinned: provider-Linear')
    const table = canvas.getByRole('table', { name: 'Records' })
    table.scrollTop = table.scrollHeight
    table.dispatchEvent(new Event('scroll'))
    await waitFor(() =>
      expect(canvas.getByTestId('pinned')).toHaveTextContent(
        `Pinned: provider-${PROVIDERS[PROVIDERS.length - 1]}`,
      ),
    )
  },
}

function NestedGroups({ depth }: { depth: number }) {
  if (depth <= 0) {
    return (
      <Row>
        <Cell>final.operation</Cell>
        <Cell mono>1</Cell>
      </Row>
    )
  }

  return (
    <Group label={`Nested group ${depth}`}>
      <NestedGroups depth={depth - 1} />
    </Group>
  )
}

export const ControlledNestedGroups: NestedGroupsStory = {
  args: { depth: 4 },
  argTypes: {
    depth: {
      control: { min: 1, max: 12, step: 1, type: 'number' },
    },
  },
  render: ({ depth }) => (
    <Container
      columns={[
        { label: 'Record', width: 'fill' },
        { align: 'right', label: 'Requests', width: 'content' },
      ]}
      layout="fixed"
    >
      <Head />
      <NestedGroups depth={depth} />
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

// Every provider and operation-group heading mounts, so the sticky groups pin
// as usual; only the operation rows near the viewport mount.
function VirtualCatalog() {
  const scrollRef = useRef<HTMLDivElement>(null)
  const providers = PROVIDERS.map((provider, p) => ({
    provider,
    groups: OPERATION_GROUPS.map((group, g) => ({
      group,
      rows: Array.from({ length: 60 }, (_row, i) => ({
        index: p * (1 + OPERATION_GROUPS.length * 61) + 1 + g * 61 + 1 + i,
        name: `${group.toLowerCase()}.${OPERATIONS[i % OPERATIONS.length]}${Math.floor(i / OPERATIONS.length) + 1}`,
      })),
    })),
  }))
  const count = PROVIDERS.length * (1 + OPERATION_GROUPS.length * 61)
  const headings = new Set<number>()
  for (const { groups } of providers) {
    for (const { rows } of groups) headings.add(rows[0]!.index - 1)
  }
  for (let p = 0; p < PROVIDERS.length; p++) headings.add(p * (1 + OPERATION_GROUPS.length * 61))
  const virtual = useVirtualRows({
    count,
    estimateSize: (index) =>
      headings.has(index) ? GROUP_HEADING_HEIGHT_PX.default : CELL_HEIGHT_PX.default,
    scrollRef,
  })
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 360 }}>
      <Container columns={SCROLL_COLUMNS} layout="scroll" ref={scrollRef}>
        <Head />
        {providers.map(({ provider, groups }) => (
          <Group key={provider} label={provider}>
            {groups.map(({ group, rows }) => (
              <Group key={group} label={group}>
                <VirtualRows rows={rows} virtual={virtual}>
                  {({ index, name }) => (
                    <Row key={name} data-index={index} ref={virtual.measureRow}>
                      <Cell mono>{name}</Cell>
                      <Cell mono>{index}</Cell>
                    </Row>
                  )}
                </VirtualRows>
              </Group>
            ))}
          </Group>
        ))}
      </Container>
    </div>
  )
}

export const VirtualRowsInNestedGroups: Story = {
  render: () => <VirtualCatalog />,
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

// A bounded table once shrank every row to the shortest row's height, so a
// wrapped cell spilled over the next row and took its clicks.
export const BoundedWrapping: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', height: 240, width: 360 }}>
      <Container ariaLabel="Features" columns={FEATURE_COLUMNS} layout="fixed" bounded>
        <Head />
        <Body>
          {Array.from({ length: 20 }, (_, index) => (
            <Row key={index}>
              <Cell wrap>
                {index === 0
                  ? 'Long-form prose in a cell wraps onto as many lines as it needs, and the row grows with it.'
                  : `Feature ${index + 1}`}
              </Cell>
              <Cell mono>on</Cell>
            </Row>
          ))}
        </Body>
      </Container>
    </div>
  ),
  play: async ({ canvasElement }) => {
    const table = within(canvasElement).getByRole('table', { name: 'Features' })
    const rows = within(table).getAllByRole('row').slice(1)
    await expect(rows[0].getBoundingClientRect().height).toBeGreaterThan(
      rows[1].getBoundingClientRect().height,
    )
    for (const row of rows) {
      const bottom = row.getBoundingClientRect().bottom
      for (const cell of within(row).getAllByRole('cell'))
        await expect(cell.getBoundingClientRect().bottom).toBeLessThanOrEqual(bottom)
    }
  },
}

function ReadingExample() {
  const [grown, grow] = useState(false)
  const [hidden, hide] = useState(false)
  const [removed, remove] = useState(false)
  const [paused, pause] = useState(false)
  return (
    <>
      <Button.TextButton onClick={() => grow(true)}>Grow earlier row</Button.TextButton>
      <Button.TextButton onClick={() => remove(true)}>Remove visible anchor</Button.TextButton>
      <Button.TextButton onClick={() => hide(!hidden)}>Toggle visibility</Button.TextButton>
      <Button.TextButton onClick={() => pause(!paused)}>Toggle connection</Button.TextButton>
      <div style={{ display: hidden ? 'none' : 'flex', height: 240 }}>
        <Container
          ariaLabel="Reading window"
          columns={[{ label: 'Record', width: 'fill' }]}
          layout="scroll"
          readingKey="reading"
          readingPaused={paused}
          density="default"
        >
          <Head />
          <Body>
            {Array.from({ length: paused ? 0 : 30 }, (_, index) => index)
              .filter((index) => !removed || index !== 10)
              .map((index) => (
                <Row
                  key={index}
                  recordKey={String(index)}
                  style={{ minHeight: grown && index === 0 ? 160 : index % 2 ? 48 : 32 }}
                >
                  <Cell>Record {index}</Cell>
                </Row>
              ))}
          </Body>
        </Container>
      </div>
    </>
  )
}
export const LiveReadingWindow: Story = {
  render: () => <ReadingExample />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const port = canvas.getByRole('table', { name: 'Reading window' })
    const target = canvas.getByText('Record 10').closest('[role=row]')!
    // Hide the preceding row completely under the sticky heading, so Record 10 really
    // is the first visible anchor (not a sliver of Record 9).
    const headingBottom = canvas
      .getByRole('columnheader', { name: 'Record' })
      .getBoundingClientRect().bottom
    port.scrollTop = target.getBoundingClientRect().top - headingBottom + 1
    port.dispatchEvent(new Event('scroll'))
    const survivor = canvas.getByText('Record 11').closest('[role=row]')!
    const offset = () => survivor.getBoundingClientRect().top - port.getBoundingClientRect().top
    const before = offset()
    await userEvent.click(canvas.getByRole('button', { name: 'Grow earlier row' }))
    await waitFor(() => expect(Math.abs(offset() - before)).toBeLessThan(1))
    await userEvent.click(canvas.getByRole('button', { name: 'Remove visible anchor' }))
    await waitFor(() => expect(Math.abs(offset() - before)).toBeLessThan(1))
    await userEvent.click(canvas.getByRole('button', { name: 'Toggle visibility' }))
    await userEvent.click(canvas.getByRole('button', { name: 'Toggle visibility' }))
    await waitFor(() => expect(Math.abs(offset() - before)).toBeLessThan(1))
    await userEvent.click(canvas.getByRole('button', { name: 'Toggle connection' }))
    await userEvent.click(canvas.getByRole('button', { name: 'Toggle connection' }))
    await waitFor(() =>
      expect(
        Math.abs(
          canvas.getByText('Record 11').closest('[role=row]')!.getBoundingClientRect().top -
            port.getBoundingClientRect().top -
            before,
        ),
      ).toBeLessThan(1),
    )
  },
}

function LoadMoreExample() {
  const [first, setFirst] = useState(0)
  const [last, setLast] = useState(19)
  const [attempts, setAttempts] = useState(0)
  const [visible, setVisible] = useState<string[]>([])
  // The first older load fails, so the story shows the retry.
  const failed = attempts === 1
  const loadOlder = () => {
    setAttempts((count) => count + 1)
    if (attempts !== 0) setLast((index) => index + 10)
  }
  return (
    <>
      <output aria-label="Records on screen">{visible.join(' ')}</output>
      <div style={{ height: 300, display: 'flex' }}>
        <Container
          ariaLabel="Loading reading"
          columns={[{ label: 'Record', width: 'fill' }]}
          density="compact"
          layout="scroll"
          readingKey="loading"
          onVisibleRecordsChange={setVisible}
        >
          <Head />
          <Body>
            {first > -10 && (
              <LoadMore
                onLoad={() => setFirst((index) => index - 10)}
                retryLabel="Try again"
                loadingLabel="Loading newer records…"
              >
                Load newer records
              </LoadMore>
            )}
            {Array.from({ length: last - first + 1 }, (_, index) => first + index).map((index) => (
              <Row key={index} recordKey={String(index)}>
                <Cell>Record {index}</Cell>
              </Row>
            ))}
            {last < 39 && (
              <LoadMore
                automatic
                onLoad={loadOlder}
                error={failed && 'The records did not load.'}
                retryLabel="Try again"
                loadingLabel="Loading older records…"
              >
                Load older records
              </LoadMore>
            )}
          </Body>
        </Container>
      </div>
    </>
  )
}
/**
 * Rows load at either end of a reading table. Near the end, the older rows load by
 * themselves; a failed load waits for a retry. Rows that load above do not move the
 * rows on screen, and the table reports which records are on screen.
 */
export const LoadMoreRows: Story = {
  render: () => <LoadMoreExample />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const port = canvas.getByRole('table', { name: 'Loading reading' })
    const onScreen = () => canvas.getByRole('status', { name: 'Records on screen' }).textContent
    const row = (index: number) => canvas.getByText(`Record ${index}`).closest('[role=row]')!
    await waitFor(() => expect(onScreen()).toMatch(/^0 1 2 /))
    // Scroll Record 10 to the top, just under the column heading.
    const headingBottom = canvas
      .getByRole('columnheader', { name: 'Record' })
      .getBoundingClientRect().bottom
    port.scrollTop += row(10).getBoundingClientRect().top - headingBottom
    await waitFor(() => expect(onScreen()).toMatch(/^10 11 /))

    // Focus scrolls the button into view; the rows then on screen must stay put.
    canvas.getByRole('button', { name: 'Load newer records' }).focus()
    const offset = () => row(5).getBoundingClientRect().top - port.getBoundingClientRect().top
    const before = offset()
    await userEvent.keyboard('{Enter}')
    await canvas.findByText('Record -1')
    await waitFor(() => expect(Math.abs(offset() - before)).toBeLessThan(1))
    await expect(canvas.queryByRole('button', { name: 'Load newer records' })).toBeNull()

    port.scrollTop = port.scrollHeight
    await expect(await canvas.findByRole('alert')).toHaveTextContent('The records did not load.')
    await expect(canvas.queryByText('Record 20')).toBeNull()
    await userEvent.click(canvas.getByRole('button', { name: 'Try again' }))
    await canvas.findByText('Record 29')
    port.scrollTop = port.scrollHeight
    await canvas.findByText('Record 39')
    await waitFor(() =>
      expect(canvas.queryByRole('button', { name: 'Load older records' })).toBeNull(),
    )
  },
}

function LoadMoreFocusExample() {
  const [loading, setLoading] = useState(false)
  return (
    <div style={{ height: 160, display: 'flex' }}>
      <Container
        ariaLabel="Loading focus"
        columns={[{ label: 'Record', width: 'fill' }]}
        density="compact"
        layout="scroll"
      >
        <Head />
        <Body>
          <Row>
            <Cell>Record 1</Cell>
          </Row>
          <LoadMore
            loading={loading}
            onLoad={() => {
              setLoading(true)
              setTimeout(() => setLoading(false), 300)
            }}
            retryLabel="Try again"
            loadingLabel="Loading older records…"
          >
            Load older records
          </LoadMore>
        </Body>
      </Container>
    </div>
  )
}
/**
 * A load in flight is a spinner, not a button; a screen reader reads its label. A
 * keyboard user who starts it keeps focus on the row: on the status while it loads,
 * then on the button again.
 */
export const LoadMoreFocus: Story = {
  render: () => <LoadMoreFocusExample />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    canvas.getByRole('button', { name: 'Load older records' }).focus()
    await userEvent.keyboard('{Enter}')
    const status = await canvas.findByRole('status')
    await expect(status).toHaveTextContent('Loading older records…')
    // Sighted readers see only the spinner: the label takes no visible space.
    await expect(
      canvas.getByText('Loading older records…').getBoundingClientRect().width,
    ).toBeLessThanOrEqual(1)
    await expect(canvas.queryByRole('button', { name: /records/ })).toBeNull()
    await expect(status).toHaveFocus()
    await waitFor(() =>
      expect(canvas.getByRole('button', { name: 'Load older records' })).toHaveFocus(),
    )
  },
}

function PinnedReadingExample() {
  const [inserted, insert] = useState(0)
  const [reveal, setReveal] = useState<string>()
  return (
    <>
      <Button.TextButton onClick={() => insert((count) => count + 5)}>
        Insert earlier children
      </Button.TextButton>
      <Button.TextButton onClick={() => setReveal('parent')}>Reveal the parent</Button.TextButton>
      <Button.TextButton onClick={() => setReveal('Child 30')}>Reveal Child 30</Button.TextButton>
      <div style={{ height: 300, display: 'flex' }}>
        <Container
          ariaLabel="Pinned reading"
          columns={[{ label: 'Record', width: 'fill' }]}
          density="compact"
          layout="scroll"
          readingKey="pinned"
          revealRecord={reveal}
        >
          <Head />
          <Body>
            <Row recordKey="before">
              <Cell>Before</Cell>
            </Row>
            <Group
              label="Parent"
              heading={<Cell>Parent</Cell>}
              headingRow={{ recordKey: 'parent' }}
            >
              {[
                ...Array.from({ length: inserted }, (_, index) => `Earlier ${index + 1}`),
                ...Array.from({ length: 40 }, (_, index) => `Child ${index + 1}`),
              ].map((label) => (
                <Row key={label} recordKey={label}>
                  <Cell>{label}</Cell>
                </Row>
              ))}
            </Group>
            {Array.from({ length: 20 }, (_, index) => (
              <Row key={index} recordKey={`after-${index}`}>
                <Cell>After {index + 1}</Cell>
              </Row>
            ))}
          </Body>
        </Container>
      </div>
    </>
  )
}
/**
 * A pinned record heading stays put while its rows move, so reading position must not
 * anchor on it, and revealing it scrolls to where its record starts. A row revealed
 * from under the heading comes out just beneath it.
 */
export const PinnedReadingAnchor: Story = {
  render: () => <PinnedReadingExample />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const port = canvas.getByRole('table', { name: 'Pinned reading' })
    const top = (text: string) =>
      within(port)
        .getAllByRole('row')
        .find((row) => within(row).queryByText(text))!
        .getBoundingClientRect().top - port.getBoundingClientRect().top
    // Scroll into the group so its heading pins over Child 20 and its neighbours.
    port.scrollTop += top('Child 20') - 120
    port.dispatchEvent(new Event('scroll'))
    await waitFor(() => expect(top('Parent')).toBeLessThan(top('Child 20')))
    const before = top('Child 20')
    await userEvent.click(canvas.getByRole('button', { name: 'Insert earlier children' }))
    await waitFor(() => expect(Math.abs(top('Child 20') - before)).toBeLessThan(1))

    // Leave the group, then reveal it: its first rows come into view, not its last.
    port.scrollTop = port.scrollHeight
    port.dispatchEvent(new Event('scroll'))
    await userEvent.click(canvas.getByRole('button', { name: 'Reveal the parent' }))
    await waitFor(() => expect(within(port).getByText('Earlier 1')).toBeVisible())
    await waitFor(() => expect(top('Earlier 1')).toBeLessThan(port.clientHeight))
    await expect(top('Earlier 1')).toBeGreaterThan(0)

    // Child 30 sits under the pinned heading. Revealing it moves it out, and no further.
    port.scrollTop += top('Child 30') - top('Parent')
    port.dispatchEvent(new Event('scroll'))
    const heading = () =>
      within(port)
        .getAllByRole('row')
        .find((row) => within(row).queryByText('Parent'))!
    const beneath = () =>
      heading().getBoundingClientRect().bottom - port.getBoundingClientRect().top
    await waitFor(() => expect(top('Child 30')).toBeLessThan(beneath() - 4))
    await userEvent.click(canvas.getByRole('button', { name: 'Reveal Child 30' }))
    await waitFor(() => expect(top('Child 30')).toBeGreaterThanOrEqual(beneath()))
    await expect(top('Child 30')).toBeLessThanOrEqual(beneath() + 1)
  },
}

function ContinuousExample({ total = 350 }: { total?: number }) {
  const [offset, setOffset] = useState(0)
  const [revision, setRevision] = useState(0)
  const [requests, setRequests] = useState(0)
  const port = useRef<HTMLDivElement>(null)
  const items = Array.from({ length: Math.min(100, total - offset) }, (_, index) => ({
    key: String(offset + index),
    index: offset + index,
    content: (
      <Row recordKey={String(offset + index)}>
        <Cell>Record {offset + index}</Cell>
      </Row>
    ),
  }))
  return (
    <>
      <output aria-label="Window requests">{requests}</output>
      <Button.TextButton
        onClick={() => {
          port.current!.scrollTop = port.current!.scrollHeight
          setRevision((value) => value + 1)
        }}
      >
        Scroll and update
      </Button.TextButton>
      <div style={{ height: 360, display: 'flex' }}>
        <Container
          ariaLabel="Continuous records"
          ref={port}
          columns={[{ label: 'Record', width: 'fill' }]}
          layout="scroll"
          readingKey="continuous"
        >
          <Head />
          <Body>
            <ReadingWindow
              label={`Children ${revision}`}
              total={total}
              size={100}
              onViewportChange={(viewport) => {
                setRequests((value) => value + 1)
                setOffset(viewport.offset)
              }}
              items={items}
            />
          </Body>
        </Container>
      </div>
    </>
  )
}
export const ContinuousHistory: Story = {
  render: () => <ContinuousExample />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const port = canvas.getByRole('table', { name: 'Continuous records' })
    await userEvent.click(canvas.getByRole('button', { name: 'Scroll and update' }))
    await waitFor(() => expect(canvas.getByText('Record 349')).toBeVisible())
    expect(port.querySelectorAll('[data-record-key]').length).toBeLessThanOrEqual(100)
    port.scrollTop = 0
    port.dispatchEvent(new Event('scroll'))
    await waitFor(() => expect(canvas.getByText('Record 0')).toBeVisible())
    expect(port.querySelectorAll('[data-record-key]').length).toBeLessThanOrEqual(100)
    const frame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()))
    await frame()
    await frame()
    const requests = canvas.getByLabelText('Window requests').textContent
    port.scrollTop = 120
    port.dispatchEvent(new Event('scroll'))
    await frame()
    await frame()
    expect(canvas.getByLabelText('Window requests').textContent).toBe(requests)
  },
}

export const VeryLongHistory: Story = {
  render: () => <ContinuousExample total={5_000_000} />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const port = canvas.getByRole('table', { name: 'Continuous records' })
    await userEvent.click(canvas.getByRole('button', { name: 'Scroll and update' }))
    await waitFor(() => expect(canvas.getByText('Record 4999999')).toBeVisible())
    expect(port.querySelectorAll('[data-record-key]').length).toBeLessThanOrEqual(100)
    expect(port.scrollHeight).toBeLessThan(600_000)
  },
}

// A Run remains expanded outside its parent's ordinary materialized window.
function NestedHistoryExample() {
  const [followStart, setFollowStart] = useState(true)
  const [loaded, setLoaded] = useState({ task: 0, run: 0 })
  const pending = useRef<ReturnType<typeof setTimeout>>(undefined)
  const viewports = useRef<Record<string, WindowViewport>>({})
  useEffect(() => () => clearTimeout(pending.current), [])
  const request = (parent: string, value: WindowViewport) => {
    viewports.current = { ...viewports.current, [parent]: value }
    clearTimeout(pending.current)
    pending.current = setTimeout(
      () =>
        setLoaded({
          task: viewports.current.task?.offset ?? 0,
          run: viewports.current.run?.offset ?? 0,
        }),
      150,
    )
  }
  const { task: taskOffset, run: runOffset } = loaded
  const port = useRef<HTMLDivElement>(null)
  const outputs = Array.from({ length: Math.min(100, 300 - runOffset) }, (_, index) => ({
    key: `output-${runOffset + index}`,
    index: runOffset + index,
    content: (
      <Row recordKey={`output-${runOffset + index}`}>
        <Cell>Emission {runOffset + index}</Cell>
      </Row>
    ),
  }))
  const run = (
    <>
      <Row recordKey="run">
        <Cell>Expanded run</Cell>
      </Row>
      <ReadingWindow
        label="Execution output"
        total={300}
        size={100}
        onViewportChange={(value) => request('run', value)}
        items={outputs}
      />
    </>
  )
  const indexes = [
    ...new Set([
      ...Array.from({ length: Math.min(100, 351 - taskOffset) }, (_, index) => taskOffset + index),
      200,
    ]),
  ]
  return (
    <div style={{ height: 360, display: 'flex' }}>
      <Container
        ariaLabel="Nested history"
        ref={port}
        columns={[{ label: 'Record', width: 'fill' }]}
        layout="scroll"
        readingKey="nested"
        readingFollowStart={followStart}
        onReadingStartChange={setFollowStart}
      >
        <Head />
        <Body>
          <Row recordKey="task">
            <Cell>Task</Cell>
          </Row>
          <ReadingWindow
            label="Task attempts"
            total={351}
            size={100}
            onViewportChange={(value) => request('task', value)}
            items={indexes.map((index) => ({
              key: index === 200 ? 'run' : `attempt-${index}`,
              index,
              content:
                index === 200 ? (
                  run
                ) : (
                  <Row recordKey={`attempt-${index}`}>
                    <Cell>Attempt {index}</Cell>
                  </Row>
                ),
            }))}
          />
        </Body>
      </Container>
    </div>
  )
}
export const NestedContinuousHistory: Story = {
  render: () => <NestedHistoryExample />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const port = canvas.getByRole('table', { name: 'Nested history' })
    const run = canvas.getByText('Expanded run')
    run.scrollIntoView({ block: 'nearest' })
    port.dispatchEvent(new Event('scroll'))
    await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()))
    const output = canvas.getByRole('rowgroup', { name: 'Execution output' })
    port.scrollTop +=
      output.getBoundingClientRect().bottom -
      port.getBoundingClientRect().top -
      port.clientHeight +
      80
    port.dispatchEvent(new Event('scroll'))
    await waitFor(() => expect(canvas.getByText('Emission 299')).toBeVisible())
    expect(port.querySelectorAll('[data-record-key]').length).toBeLessThanOrEqual(202)
  },
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

// Rows of bare inputs read as a table. Each row's actions show under the pointer
// or focus, and the last row adds a row.
export const EditableRows: Story = {
  render: function EditableRows() {
    const [names, setNames] = useState(['dd-api-key', 'dd-application-key'])
    return (
      <Container
        ariaLabel="Headers"
        variant="card"
        columns={[
          { label: 'Header', width: 'fill' },
          { ariaLabel: 'Actions', width: 44 },
        ]}
      >
        <Head />
        <Body>
          {names.map((name, index) => (
            <Row key={index}>
              <Cell control>
                <TextInput
                  bare
                  ariaLabel="Header"
                  value={name}
                  onChange={(next) => setNames(names.map((old, at) => (at === index ? next : old)))}
                />
              </Cell>
              <Cell actions>
                <Button.IconButton
                  name="Trash"
                  variant="bare"
                  tooltipText={`Remove ${name || 'header'}`}
                  onClick={() => setNames(names.filter((_name, at) => at !== index))}
                />
              </Cell>
            </Row>
          ))}
          <AddRow onClick={() => setNames([...names, ''])}>Add header</AddRow>
        </Body>
      </Container>
    )
  },
}
