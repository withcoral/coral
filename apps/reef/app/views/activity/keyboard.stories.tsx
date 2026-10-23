import type { Meta, StoryObj } from '@storybook/react'
import { createRoutesStub, useLoaderData, useLocation, useNavigate } from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { activityPage, runRow, taskRow, toolRow } from './fixtures'
import type { ActivityPage, ActivityRow } from './types'
import {
  click,
  emulateReducedMotion,
  inTestBrowser,
  press,
  resizeViewport,
} from '../../../.storybook/browser'

// Real keypresses in Chromium: focus order, what the reader sees, and keys that
// race a record load. Key ownership and traversal rules are keyboard.test.tsx's
// and navigation.test.ts's.

const collect = taskRow('collect', 'Collect invoices')
const calls = Array.from({ length: 30 }, (_, index) =>
  toolRow(collect.name, `c${index + 1}`, { intent: `Call ${index + 1}`, position: index }),
)
const tasks = [1, 2, 3].map((index) => taskRow(`t${index}`, `Task ${index}`))
const long = runRow('long', '', { actor: 'Long run', intent: 'Long run' })
const roots: ActivityRow[] = [collect, ...tasks, long]

/** The page the loader returns for the URL: its selection and expanded records. */
function pageFor(url: URL): ActivityPage {
  const record = url.searchParams.get('record')
  const expanded = url.searchParams.has('tree') ? url.searchParams.getAll('expand') : []
  const selectedRow = [...roots, ...calls].find((row) => row.name === record) ?? null
  return activityPage({
    rows: roots,
    expanded,
    branches: expanded.includes(collect.name)
      ? { [collect.name]: { rows: calls, total: '30', next: '', unavailable: false } }
      : {},
    selected: selectedRow?.name ?? null,
    selectedRow,
    detail:
      selectedRow?.kind === 'task'
        ? { kind: 'task', task: selectedRow }
        : selectedRow?.kind === 'tool'
          ? {
              kind: 'tool',
              task: selectedRow.task,
              interaction: selectedRow,
              evidence: { kind: 'unknown' },
            }
          : selectedRow?.kind === 'run'
            ? {
                kind: 'run',
                run: selectedRow,
                source: Array.from({ length: 120 }, (_, line) => `// line ${line}`).join('\n'),
                linkedFunctions: [],
                // A tall final value: the Run panel shows it open, unlike its source.
                terminal: {
                  kind: 'succeeded',
                  finalValue: JSON.stringify(
                    Array.from({ length: 60 }, (_, line) => `line ${line}`),
                    null,
                    2,
                  ),
                },
              }
            : null,
  })
}

/** Holds record loads until released, so a play can press keys while one is pending. */
const loads = {
  finished: 0,
  held: null as Promise<void> | null,
  release: () => {},
  hold() {
    this.held = new Promise((resolve) => (this.release = resolve))
  },
  open() {
    this.release()
    this.held = null
  },
}

function KeyboardRoute() {
  const page = useLoaderData<ActivityPage>()
  const { search } = useLocation()
  const navigate = useNavigate()
  return (
    <ActionReviewSession request={null}>
      <code aria-label="Address">{search}</code>
      <button type="button" onClick={() => navigate(-1)}>
        History back
      </button>
      <button type="button">Before the table</button>
      {/* The live route always names its reading window. */}
      <Activity page={page} windowKey="keyboard" />
    </ActionReviewSession>
  )
}

const meta = {
  title: 'Activity/Keyboard',
  parameters: { layout: 'fullscreen', initialEntry: '/workspaces/finance/activity' },
  decorators: [
    (_, context) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: KeyboardRoute,
          HydrateFallback: () => null,
          loader: async ({ request }) => {
            if (loads.held) await loads.held
            loads.finished++
            return pageFor(new URL(request.url))
          },
        },
      ])
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
            <Stub initialEntries={[context.parameters.initialEntry]} />
          </PageLayout>
        </div>
      )
    },
  ],
  render: () => <></>,
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

async function surface(canvasElement: HTMLElement) {
  const canvas = within(canvasElement)
  const screen = within(canvasElement.ownerDocument.body)
  const table = await canvas.findByRole('table', { name: 'Activity records', hidden: true })
  const row = (label: string) =>
    within(table)
      .getByRole('link', { name: label, hidden: true })
      .closest<HTMLElement>('[role="row"]')!
  return {
    canvas,
    screen,
    table,
    row,
    address: () => canvas.getByLabelText('Address').textContent ?? '',
    rail: () => screen.queryByRole('dialog', { name: 'Activity details' }),
    /** The label of the row the cursor shows on. */
    cursor: () => table.querySelector('[data-active] a[href]')?.textContent ?? null,
    heading: (rail: HTMLElement, name: string) =>
      waitFor(() => expect(within(rail).getByRole('heading', { name, level: 2 })).toBeVisible()),
  }
}

/** Waits until every load started so far has landed, and the page has painted it. */
async function landed(count: number) {
  await waitFor(() => expect(loads.finished).toBeGreaterThanOrEqual(count))
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)))
}

/** Real Tab and Shift+Tab, the focus indicators, and keys that keep focus where they act. */
export const FocusOrder: Story = {
  play: async ({ canvasElement, userEvent }) => {
    loads.open()
    if (!(await inTestBrowser())) return
    const { canvas, table, row, rail, cursor, address, heading } = await surface(canvasElement)
    const before = canvas.getByRole('button', { name: 'Before the table' })
    await userEvent.click(before)
    // A focused control keeps its keys.
    await press('{ArrowDown}')
    await expect(before).toHaveFocus()
    await expect(cursor()).toBeNull()
    // Tab reaches the live toggle beside the list heading, then the table itself, which shows
    // its focus until a cursor row does.
    await press('{Tab}')
    await expect(canvas.getByRole('button', { name: 'Follow latest activity' })).toHaveFocus()
    await press('{Tab}')
    await expect(table).toHaveFocus()
    await expect(getComputedStyle(table).outlineStyle).not.toBe('none')
    await press('{ArrowDown}')
    await expect(cursor()).toBe('Collect invoices')
    await expect(getComputedStyle(row('Collect invoices')).boxShadow).not.toBe('none')
    await expect(getComputedStyle(table).outlineStyle).toBe('none')
    // The table is one Tab stop: Tab leaves it, past its records and their controls.
    await press('{Tab}')
    await expect(table.contains(document.activeElement)).toBe(false)
    await press('{Shift>}{Tab}{/Shift}')
    await expect(table).toHaveFocus()
    await expect(cursor()).toBe('Collect invoices')

    // Enter opens the cursor record beside the table, and focus stays in the table.
    await press('{ArrowDown}{Enter}')
    const opened = await within(document.body).findByRole('dialog', { name: 'Activity details' })
    await heading(opened, 'Task 1')
    await expect(table).toHaveFocus()
    await expect(row('Task 1')).toHaveAttribute('aria-selected', 'true')
    await expect(cursor()).toBe('Task 1')
    await press('{ArrowDown}')
    await heading(opened, 'Task 2')
    await expect(table).toHaveFocus()
    // Tab goes from the table to the rail's first control, and Shift+Tab comes back.
    const follow = within(opened).getByRole('button', { name: 'Follow this task' })
    await press('{Tab}')
    await expect(follow).toHaveFocus()
    await press('{Shift>}{Tab}{/Shift}')
    await expect(table).toHaveFocus()

    // ↓ on the rail itself steps the record in the same rail, and focus stays.
    await userEvent.click(within(opened).getByRole('heading', { name: 'Task 2', level: 2 }))
    await waitFor(() => expect(opened).toHaveFocus())
    await press('{ArrowDown}')
    await heading(opened, 'Task 3')
    await expect(rail()).toBe(opened)
    await expect(opened).toHaveFocus()
    await expect(address()).toContain('t3')
    // Escape closes the rail and returns focus to the table, on the record it showed.
    await press('{Escape}')
    await waitFor(() => expect(rail()).toBeNull())
    await waitFor(() => expect(table).toHaveFocus())
    await expect(cursor()).toBe('Task 3')
    await press('{ArrowUp}')
    await expect(cursor()).toBe('Task 2')
    await expect(address()).not.toContain('record=')
  },
}

/** The cursor row shows in full beneath the pinned headings, and new detail starts at its top. */
export const RevealAndDetailScroll: Story = {
  parameters: { initialEntry: `/workspaces/finance/activity?tree=1&expand=${collect.name}` },
  play: async ({ canvasElement }) => {
    loads.open()
    if (!(await resizeViewport(1200, 420))) return
    const { table, row, cursor, rail, heading } = await surface(canvasElement)
    const visibleTop = (label: string) => {
      // Every heading pinned above the row: the column heading, the day and its Task.
      const pinned = [...table.querySelectorAll<HTMLElement>('[data-pinned-heading]')].filter(
        (pinnedRow) => !pinnedRow.contains(row(label)) && pinnedRow !== row(label),
      )
      const head = table.querySelector('[role="columnheader"]')!.getBoundingClientRect().bottom
      return Math.max(
        head,
        ...pinned
          .map((pinnedRow) => pinnedRow.getBoundingClientRect())
          .filter((box) => box.top < row(label).getBoundingClientRect().top + 1)
          .map((box) => box.bottom),
      )
    }
    const inView = (label: string) => {
      const box = row(label).getBoundingClientRect()
      const port = table.getBoundingClientRect()
      return box.top >= visibleTop(label) - 1 && box.bottom <= port.top + table.clientHeight + 1
    }
    table.focus()
    await press('{ArrowDown}'.repeat(16))
    await expect(cursor()).toBe('Call 15')
    await waitFor(() => expect(inView('Call 15')).toBe(true))
    // Back up from the end: the row comes out from under the pinned headings.
    table.scrollTop = table.scrollHeight
    table.dispatchEvent(new Event('scroll'))
    await press('{ArrowUp}')
    await expect(cursor()).toBe('Call 14')
    await waitFor(() => expect(inView('Call 14')).toBe(true))
    await expect(row('Call 14').getBoundingClientRect().top).toBeLessThanOrEqual(
      visibleTop('Call 14') + 1,
    )

    // A new record in the rail starts at its top.
    await press('{ArrowDown}'.repeat(20))
    await expect(cursor()).toBe('Long run')
    await press('{Enter}')
    const opened = await within(document.body).findByRole('dialog', { name: 'Activity details' })
    await heading(opened, 'Long run')
    // A short window leaves the detail taller than the rail.
    await resizeViewport(1200, 240)
    await waitFor(() => expect(opened.scrollHeight).toBeGreaterThan(opened.clientHeight + 50))
    opened.scrollTop = opened.scrollHeight
    await waitFor(() => expect(opened.scrollTop).toBeGreaterThan(0))
    // Focus the rail itself, as a click on its text does.
    opened.focus()
    await waitFor(() => expect(opened).toHaveFocus())
    await press('{ArrowUp}')
    await heading(opened, 'Task 3')
    await expect(rail()).toBe(opened)
    await expect(opened.scrollTop).toBe(0)
  },
}

/** Escape and clicks that race a pending step, under normal and reduced motion. */
export const KeysDuringLoads: Story = {
  play: async ({ canvasElement, userEvent }) => {
    loads.open()
    if (!(await inTestBrowser())) return
    const { table, row, rail, cursor, address, heading } = await surface(canvasElement)
    const open = async (label: string) => {
      loads.open()
      await userEvent.click(within(row(label)).getByRole('link'))
      const opened = await within(document.body).findByRole('dialog', {
        name: 'Activity details',
      })
      await heading(opened, label)
      // The rail takes focus as it opens; let that land before moving it. A click
      // on the open record's row then returns focus to the table.
      await waitFor(() => expect(opened.contains(document.activeElement)).toBe(true))
      await userEvent.click(within(row(label)).getByRole('link'))
      await waitFor(() => expect(table.contains(document.activeElement)).toBe(true))
      return opened
    }
    for (const reduced of [false, true]) {
      await emulateReducedMotion(reduced)
      try {
        // Escape during a pending step closes once; the late load does not reopen.
        await open('Task 1')
        const started = loads.finished
        loads.hold()
        // ↓ and Escape in one burst: the close lands before the step registers.
        await press('{ArrowDown}{Escape}')
        await waitFor(() => expect(rail()).toBeNull())
        loads.open()
        await landed(started + 1)
        await expect(rail()).toBeNull()
        await expect(address()).not.toContain('record=')
        // Focus and the cursor return to the record the rail last showed.
        await waitFor(() => expect(table.contains(document.activeElement)).toBe(true))
        await expect(cursor()).toBe('Task 1')
        // Back reopens the record; the cancelled step is gone, so ↓ starts from it.
        await userEvent.click(within(canvasElement).getByRole('button', { name: 'History back' }))
        await waitFor(() => expect(rail()?.contains(document.activeElement)).toBe(true))
        await heading(rail()!, 'Task 1')
        table.focus()
        await press('{ArrowDown}')
        await heading(rail()!, 'Task 2')
        await press('{Escape}')
        await waitFor(() => expect(rail()).toBeNull())

        // Rapid steps each start from the step before, not from the record shown.
        await open('Task 1')
        loads.hold()
        await press('{ArrowDown}{ArrowDown}')
        loads.open()
        const opened = rail()!
        await heading(opened, 'Task 3')
        await expect(rail()).toBe(opened)

        // A click during a pending step wins over the step.
        loads.hold()
        await press('{ArrowUp}')
        await userEvent.click(within(row('Task 1')).getByRole('link'))
        const clicked = loads.finished
        loads.open()
        await heading(opened, 'Task 1')
        await landed(clicked + 1)
        await heading(rail()!, 'Task 1')

        // A step another navigation cancels leaves nothing behind: Space toggles the
        // step's target in place of the step, and the next key starts from the record
        // the rail still shows.
        loads.hold()
        await press('{ArrowDown}')
        await waitFor(() => expect(cursor()).toBe('Task 2'))
        await press(' ')
        loads.open()
        await waitFor(() => expect(address()).toContain('expand='))
        await waitFor(() => expect(cursor()).toBe('Task 1'))
        await heading(rail()!, 'Task 1')
        await press('{ArrowDown}')
        await heading(rail()!, 'Task 2')
        // The same, with ↓ and Space in one burst: Space replaces the step before
        // the router reports it. Space toggles Task 2 back, so the next pass starts
        // from the same list.
        await press('{Escape}')
        await waitFor(() => expect(rail()).toBeNull())
        await open('Task 1')
        loads.hold()
        await press('{ArrowDown} ')
        loads.open()
        await waitFor(() => expect(address()).not.toContain('expand='))
        await waitFor(() => expect(cursor()).toBe('Task 1'))
        await heading(rail()!, 'Task 1')
        await press('{ArrowDown}')
        await heading(rail()!, 'Task 2')
        await press('{Escape}')
        await waitFor(() => expect(rail()).toBeNull())
      } finally {
        loads.open()
        await emulateReducedMotion(false)
      }
    }
  },
}

/** At the mobile width the rail stays modal: its keys step it, and the table is out of reach. */
export const Mobile: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?record=${encodeURIComponent(tasks[0].name)}`,
  },
  play: async ({ canvasElement }) => {
    loads.open()
    if (!(await resizeViewport(390, 800))) return
    const { table, rail, address, heading } = await surface(canvasElement)
    const opened = await within(document.body).findByRole('dialog', { name: 'Activity details' })
    await waitFor(() => expect(opened.contains(document.activeElement)).toBe(true))
    opened.focus()
    await press('{ArrowDown}')
    await heading(opened, 'Task 2')
    await expect(rail()).toBe(opened)
    // Tab stays in the rail; the table behind it never takes focus.
    for (let step = 0; step < 4; step++) {
      await press('{Tab}')
      await expect(table.contains(document.activeElement)).toBe(false)
    }
    await expect(address()).toContain('t2')
    // Enter from the table opens a modal rail here, so the rail takes focus.
    await press('{Escape}')
    await waitFor(() => expect(rail()).toBeNull())
    await waitFor(() => expect(table).toHaveFocus())
    await press('{Enter}')
    const reopened = await within(document.body).findByRole('dialog', {
      name: 'Activity details',
    })
    await heading(reopened, 'Task 2')
    await waitFor(() => expect(reopened.contains(document.activeElement)).toBe(true))
  },
}

/**
 * One indicator shows where the keys act: the table holds focus and its cursor row
 * shows the record, after clicks, Enter and Escape alike. No link or table ring joins it.
 */
export const SingleFocusIndicator: Story = {
  play: async ({ canvasElement }) => {
    loads.open()
    if (!(await inTestBrowser())) return
    const { table, row, rail, heading } = await surface(canvasElement)
    const link = (label: string) => within(row(label)).getByRole('link')
    const indicatesOnly = async (label: string) => {
      await waitFor(() => expect(table).toHaveFocus())
      await waitFor(() => expect(table.querySelectorAll('[data-active]')).toHaveLength(1))
      await expect(row(label)).toHaveAttribute('data-active')
      await expect(getComputedStyle(row(label)).boxShadow).not.toBe('none')
      await expect(getComputedStyle(table).outlineStyle).toBe('none')
      await expect(table.querySelector(':focus')).toBeNull()
    }
    // A click opens the first record; the rail takes focus as it opens.
    await click(link('Task 1'))
    const opened = await within(document.body).findByRole('dialog', { name: 'Activity details' })
    await heading(opened, 'Task 1')
    await waitFor(() => expect(opened.contains(document.activeElement)).toBe(true))
    // A click on another record's link, beside the open rail.
    await click(link('Task 2'))
    await heading(opened, 'Task 2')
    await indicatesOnly('Task 2')
    // Enter with the rail open changes nothing.
    await press('{Enter}')
    await indicatesOnly('Task 2')
    // Escape closes the rail; the cursor row keeps the record.
    await press('{Escape}')
    await waitFor(() => expect(rail()).toBeNull())
    await indicatesOnly('Task 2')
    // Enter opens it again, beside the table.
    await press('{Enter}')
    await heading(
      await within(document.body).findByRole('dialog', { name: 'Activity details' }),
      'Task 2',
    )
    await indicatesOnly('Task 2')
    // A click on a row away from its link.
    await click(within(row('Task 3')).getAllByRole('cell')[0]!)
    await heading(rail()!, 'Task 3')
    await indicatesOnly('Task 3')
    await press('{Escape}')
    await waitFor(() => expect(rail()).toBeNull())
    await indicatesOnly('Task 3')
  },
}
