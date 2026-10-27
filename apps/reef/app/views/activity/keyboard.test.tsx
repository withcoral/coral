/** @vitest-environment happy-dom */
import { act, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useEffect, useState } from 'react'
import type { Location } from 'react-router'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { activityPage, approvalRequest, runRow, taskRow, toolRow, workspace } from './fixtures'
import type { ActivityPage, ActivityRow, ActivityViewProps } from './types'

// The keys Activity owns: ↑/↓, Space, Enter and Escape on the table itself (or
// its record links) and on the rail itself, and ↑/↓ from the page background.
// Traversal order and its boundaries are navigation.test.ts's.

const invoices = taskRow('invoices', 'Find invoices')
const reminders = taskRow('reminders', 'Send reminders')
const search = toolRow(invoices.name, 'search', { intent: 'Find tools', position: 0 })
const reminder = runRow('reminder', invoices.name, {
  actor: 'Reminder run',
  intent: 'Reminder run',
  state: 'running',
  endedAt: null,
  requiresAction: true,
  position: 1,
})
// The rail can show a record the list does not reach, as a direct link opens it.
const elsewhere = taskRow('elsewhere', 'Elsewhere')
const rows: ActivityRow[] = [invoices, search, reminder, reminders, elsewhere]
const list = activityPage().listPath
/** The list as live updates leave it: its roots, and the first Task's children. */
interface Lists {
  roots: ActivityRow[]
  children: ActivityRow[]
}

const rail = () => screen.queryByRole('dialog', { name: 'Activity details' })
const confirmation = () => screen.findByRole('dialog', { name: 'Discard your comment?' })

/** The page a loader returns for the URL: its selection and expanded records. */
function pageFor({ search: query }: Location, { roots, children }: Lists): ActivityPage {
  const params = new URLSearchParams(query)
  const record = params.get('record')
  const expanded = params.has('tree') ? params.getAll('expand') : [invoices.name]
  const selectedRow = [...rows, ...children].find((row) => row.name === record) ?? null
  const detail: ActivityPage['detail'] =
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
              request: approvalRequest,
              source: 'return null;',
              linkedFunctions: [],
              terminal: { kind: 'unavailable' },
            }
          : null
  return activityPage({
    rows: roots,
    expanded,
    branches: expanded.includes(invoices.name)
      ? { [invoices.name]: { rows: children, total: '2', next: '', unavailable: false } }
      : {},
    selected: selectedRow?.name ?? null,
    selectedRow,
    detail,
  })
}

async function renderActivity(entry = list, more?: ActivityViewProps['more']) {
  const user = userEvent.setup()
  let publish!: (next: Partial<Lists>) => void
  function Live({ location }: { location: Location }) {
    const [lists, setLists] = useState<Lists>({
      roots: [invoices, reminders],
      children: [search, reminder],
    })
    useEffect(() => {
      publish = (next) => setLists((value) => ({ ...value, ...next }))
    }, [])
    const page = pageFor(location, lists)
    return (
      <ActionReviewSession request={page.detail?.kind === 'run' ? page.detail.request : null}>
        <button type="button">Outside</button>
        <Activity page={page} more={more} />
      </ActionReviewSession>
    )
  }
  const rendered = renderRoute((location) => <Live location={location} />, { entry })
  const table = screen.getByRole('table', { name: 'Activity records' })
  // A rail that opens takes focus; let it, as a reader would.
  if (entry.includes('record='))
    await waitFor(() => expect(rail()?.contains(document.activeElement)).toBe(true))
  const link = (name: string) => within(table).getByRole('link', { name })
  return {
    user,
    table,
    link,
    router: rendered.router,
    /** The record the keyboard cursor rests on, by its label. */
    cursor: () => table.querySelector('[data-active] a')?.textContent ?? null,
    record: () => new URLSearchParams(rendered.router.state.location.search).get('record'),
    /** Lists `next` as a live update would. */
    update: (next: Partial<Lists>) => act(() => publish(next)),
  }
}

// Catches: the first ↓ only focusing the table, or keys acting from a focused control.
it('↑ and ↓ from the page background enter the table on the same press', async () => {
  const { user, table, cursor } = await renderActivity()
  await user.keyboard('{ArrowDown}')
  expect(table).toHaveFocus()
  expect(cursor()).toBe('Find invoices')
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Find tools')

  // A focused control keeps its keys, and the cursor stays out of sight.
  const outside = screen.getByRole('button', { name: 'Outside' })
  act(() => outside.focus())
  expect(cursor()).toBeNull()
  await user.keyboard('{ArrowDown}{ArrowUp}')
  expect(outside).toHaveFocus()
  act(() => table.focus())
  expect(cursor()).toBe('Find tools')
})

it('↑ with no cursor starts at the last record', async () => {
  const { user, table, cursor } = await renderActivity()
  await user.keyboard('{ArrowUp}')
  expect(table).toHaveFocus()
  expect(cursor()).toBe('Send reminders')
})

const opened = (name: string) => `${list}?record=${encodeURIComponent(name)}`
/** The record heading the open detail shows, after the rail's own title. */
const detailHeading = (dialog: HTMLElement | null) =>
  dialog && within(dialog).getAllByRole('heading', { level: 2 })[1]?.textContent

// Catches: a hidden cursor apart from the detail, a step that adds history, or
// a step that takes focus out of the table or the rail.
it('with the rail open, ↑ and ↓ step the shown record and keep focus where they started', async () => {
  const { user, table, link, record, router, cursor } = await renderActivity(opened(invoices.name))
  const dialog = rail()!
  // A click on another row replaces the detail; focus rests on the table.
  await user.click(link('Find tools'))
  expect(record()).toBe(search.name)
  // Focus rests on the table, whose cursor row shows the clicked record.
  expect(table).toHaveFocus()
  expect(cursor()).toBe('Find tools')
  // The next ↓ continues from the clicked record.
  await user.keyboard('{ArrowDown}')
  expect(record()).toBe(reminder.name)
  expect(router.state.historyAction).toBe('REPLACE')
  expect(table).toHaveFocus()
  expect(cursor()).toBe('Reminder run')
  expect(rail()).toBe(dialog)

  // From the page background, the open rail takes focus and steps.
  act(() => (document.activeElement as HTMLElement).blur())
  await user.keyboard('{ArrowUp}')
  expect(record()).toBe(search.name)
  expect(dialog).toHaveFocus()
  await user.keyboard('{ArrowUp}')
  expect(record()).toBe(invoices.name)
  expect(dialog).toHaveFocus()
  // A control in the rail keeps its keys.
  act(() => within(dialog).getByRole('button', { name: 'Close details' }).focus())
  await user.keyboard('{ArrowDown}')
  expect(record()).toBe(invoices.name)
})

// Catches: Space or ← → acting on the wrong record, held Space toggling again,
// or a collapse leaving the cursor or the detail on a record it hides.
it('Space toggles the cursor Task or collapses the branch around a child; ← and → do nothing', async () => {
  const { user, cursor, router, link } = await renderActivity()
  const expanded = () => new URLSearchParams(router.state.location.search).getAll('expand')
  await user.keyboard('{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe('Find tools')
  await user.keyboard('{ArrowLeft}{ArrowRight}')
  expect(link('Find tools')).toBeInTheDocument()
  // A child collapses its Task, and the cursor moves to the Task.
  await user.keyboard('[Space]')
  await waitFor(() => expect(cursor()).toBe('Find invoices'))
  expect(screen.queryByRole('link', { name: 'Find tools' })).toBeNull()
  expect(expanded()).toEqual([])
  // Held Space toggles once.
  await user.keyboard('[Space>3/]')
  expect(expanded()).toEqual([invoices.name])
  await user.keyboard('{ArrowDown}{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe('Send reminders')
  // A Task with no loaded children still toggles itself.
  await user.keyboard('[Space]')
  expect(expanded()).toEqual([invoices.name, reminders.name])
})

it('Space does not expand past the bound', async () => {
  const many = Array.from({ length: 20 }, (_, index) => `&expand=${workspace}/tasks/x${index}`)
  const { user, cursor, router } = await renderActivity(`${list}?tree=1${many.join('')}`)
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Find invoices')
  const before = router.state.location.search
  await user.keyboard('[Space]')
  expect(router.state.location.search).toBe(before)
})

// Catches: a collapse that hides the open record leaving the detail on it, or
// one that does not hide it changing the detail.
it('a collapse that hides the shown record opens the record it collapses', async () => {
  const { user, table, record, router } = await renderActivity(opened(search.name))
  act(() => table.focus())
  // Space on the shown child, from the table: one replaced address.
  await user.keyboard('[Space]')
  expect(record()).toBe(invoices.name)
  expect(router.state.historyAction).toBe('REPLACE')
  expect(screen.queryByRole('link', { name: 'Find tools' })).toBeNull()
  expect(detailHeading(rail())).toBe('Find invoices')

  // The expand control pushes, and a collapse that hides nothing keeps the detail.
  await user.click(screen.getAllByRole('button', { name: 'Expand record' })[0]!)
  await user.click(within(table).getByRole('link', { name: 'Reminder run' }))
  expect(record()).toBe(reminder.name)
  await user.click(screen.getAllByRole('button', { name: 'Expand record' })[1]!)
  expect(record()).toBe(reminder.name)
  // A pointer collapse that hides it pushes, as other clicks do.
  await user.click(screen.getAllByRole('button', { name: 'Collapse record' })[0]!)
  expect(record()).toBe(invoices.name)
  expect(router.state.historyAction).toBe('PUSH')
})

// Catches: rail Space collapsing a parent, or closing the detail.
it('Space in the rail toggles only the Task or Run it shows', async () => {
  const { user, record, router } = await renderActivity(opened(search.name))
  const dialog = rail()!
  act(() => dialog.focus())
  const before = router.state.location.search
  await user.keyboard('[Space]')
  expect(router.state.location.search).toBe(before)
  await user.keyboard('{ArrowUp}')
  expect(record()).toBe(invoices.name)
  await user.keyboard('[Space]')
  expect(new URLSearchParams(router.state.location.search).getAll('expand')).toEqual([])
  expect(record()).toBe(invoices.name)
  expect(dialog).toHaveFocus()
  expect(rail()).toBe(dialog)
})

// Catches: Escape outside the table's ownership closing the rail, or the cursor
// returning to a record other than the last one shown.
it('Escape from the table closes the rail onto the last record it showed; Enter opens it again', async () => {
  const { user, table, record, cursor, router } = await renderActivity(opened(search.name))
  act(() => screen.getByRole('button', { name: 'Outside' }).focus())
  await user.keyboard('{Escape}')
  expect(record()).toBe(search.name)
  act(() => table.focus())
  await user.keyboard('{ArrowDown}')
  await user.keyboard('{Escape}')
  expect(record()).toBeNull()
  await waitFor(() => expect(rail()).toBeNull())
  await waitFor(() => expect(table.contains(document.activeElement)).toBe(true))
  expect(cursor()).toBe('Reminder run')
  act(() => table.focus())
  await user.keyboard('{Enter}')
  expect(record()).toBe(reminder.name)
  expect(router.state.historyAction).toBe('PUSH')
})

// Catches: a cursor kept by index, so arrivals retarget it, or one that outlives its record.
it('the cursor keeps its record across live arrivals, follows acceptance, and clears on removal', async () => {
  const { user, table, cursor, update } = await renderActivity()
  await user.keyboard('{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe('Find tools')
  update({ roots: [taskRow('newer', 'Newer task'), invoices, reminders] })
  expect(cursor()).toBe('Find tools')
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Reminder run')

  // A submission becomes the Run it was accepted as, in the same place.
  const submitted = { ...search, name: `${invoices.name}/interactions/exec`, intent: 'Run it' }
  update({ children: [submitted, reminder] })
  await user.keyboard('{ArrowUp}')
  expect(cursor()).toBe('Run it')
  const accepted = {
    ...reminder,
    name: `${workspace}/runs/accepted`,
    actor: 'Accepted run',
    position: 0,
  }
  update({ children: [{ ...accepted, submission: submitted }, reminder] })
  // Its label is still the submission's intent; the cursor rests on the Run itself.
  expect(table.querySelector('[data-active]')).toHaveAttribute('data-record-key', accepted.name)

  // A record that leaves the table takes the cursor with it.
  update({ children: [reminder] })
  expect(cursor()).toBeNull()
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Newer task')
})

// Catches: a key switching away from an unsent comment without asking, or the
// confirmation's own keys driving the table behind it.
it('a step or Escape with an unsent comment asks first', async () => {
  const { user, table, record } = await renderActivity(opened(reminder.name))
  const dialog = rail()!
  await user.type(within(dialog).getByRole('textbox', { name: 'Comment (optional)' }), 'Check it')
  const choose = async (name: 'Keep editing' | 'Discard comment') => {
    await user.click(within(await confirmation()).getByRole('button', { name }))
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Discard your comment?' })).toBeNull(),
    )
  }
  act(() => table.focus())
  await user.keyboard('{ArrowDown}')
  await confirmation()
  // The confirmation keeps its keys.
  await user.keyboard('{ArrowDown}{ArrowUp}')
  await choose('Keep editing')
  expect(record()).toBe(reminder.name)
  expect(within(dialog).getByRole('textbox', { name: 'Comment (optional)' })).toHaveValue(
    'Check it',
  )

  act(() => table.focus())
  await user.keyboard('{Escape}')
  await choose('Keep editing')
  expect(record()).toBe(reminder.name)
  expect(rail()).toBe(dialog)

  act(() => table.focus())
  await user.keyboard('{ArrowDown}')
  await choose('Discard comment')
  expect(record()).toBe(reminders.name)
})

it('Discard after Escape closes the rail onto the record it showed', async () => {
  const { user, table, record, cursor } = await renderActivity(opened(reminder.name))
  await user.type(within(rail()!).getByRole('textbox', { name: 'Comment (optional)' }), 'Check it')
  act(() => table.focus())
  await user.keyboard('{Escape}')
  await user.click(within(await confirmation()).getByRole('button', { name: 'Discard comment' }))
  await waitFor(() => expect(record()).toBeNull())
  await waitFor(() => expect(rail()).toBeNull())
  act(() => table.focus())
  expect(cursor()).toBe('Reminder run')
})

// Catches regressions of withcoral/lagoon#712: Enter or Space on a focused control
// acting on the cursor record instead of the control.
it("Enter and Space on a focused control stay the control's own", async () => {
  const { user, cursor, record, router } = await renderActivity()
  const expanded = () => new URLSearchParams(router.state.location.search).getAll('expand')
  // The cursor rests on a collapsed Task, so a key that leaks to the table would
  // expand it or open it.
  await user.keyboard('{ArrowUp}')
  expect(cursor()).toBe('Send reminders')
  act(() => screen.getByRole('button', { name: 'Outside' }).focus())
  await user.keyboard('{Enter}[Space]')
  expect(router.state.location.search).toBe('')
  // The expand control collapses its own Task once, and nothing else changes.
  act(() => screen.getByRole('button', { name: 'Collapse record' }).focus())
  await user.keyboard('[Space]')
  expect(expanded()).toEqual([])
  expect(record()).toBeNull()
})

// Catches: a glimpse taking the page's keys, or losing its records' Tab stops.
it('a glimpse takes no keys and keeps a Tab stop on each record', async () => {
  const { user, table, cursor } = await renderActivity(list, {
    to: list,
    label: 'View all activity',
    roots: 2,
  })
  await user.keyboard('{ArrowDown}')
  expect(table).not.toHaveFocus()
  expect(cursor()).toBeNull()
  expect(table).not.toHaveAttribute('tabindex')
  expect(within(table).getByRole('link', { name: 'Find invoices' })).not.toHaveAttribute('tabindex')
})

// Catches: a pointer collapse leaving the closed cursor on a record it hides.
it('a collapse with the rail closed moves the cursor to the collapsed record', async () => {
  const { user, cursor, table } = await renderActivity()
  await user.keyboard('{ArrowDown}{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe('Reminder run')
  await user.click(screen.getAllByRole('button', { name: 'Collapse record' })[0]!)
  act(() => table.focus())
  await waitFor(() => expect(cursor()).toBe('Find invoices'))
})

// Catches: a click on the synthetic outside-page row leaving focus on its link,
// where the table's keys do nothing.
it('a click on a record outside the list rests focus on the table', async () => {
  const { user, table, record } = await renderActivity(opened(elsewhere.name))
  await user.click(within(table).getByRole('link', { name: 'Elsewhere' }))
  expect(table).toHaveFocus()
  await user.keyboard('{Escape}')
  expect(record()).toBeNull()
})
