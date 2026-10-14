import type { Meta, StoryObj } from '@storybook/react'
import { createRoutesStub, useLoaderData, useLocation, useNavigate } from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { activityPage, approvalRequest, run, workspace } from './fixtures'
import type { ActivityPage, ActivityRow } from './types'
import { emulateReducedMotion, resizeViewport } from '../../../.storybook/browser'

const at = '2026-09-18T14:02:00.000Z'
const tasks = [1, 2, 3].map((index): ActivityRow & { kind: 'task' } => ({
  kind: 'task',
  name: `${workspace}/tasks/t${index}`,
  workspace,
  actor: 'Collections agent',
  intent: `Task ${index}`,
  outcome: 'open',
  startedAt: at,
  endedAt: null,
  agent: '',
}))
const waiting: ActivityRow & { kind: 'run' } = {
  ...run,
  actor: 'Reminder run',
  agent: '',
  startedAt: at,
}
const rows: ActivityRow[] = [...tasks, waiting]

/** The page the loader returns for the record the URL selects. */
function pageFor(record: string | null): ActivityPage {
  const selectedRow = rows.find((row) => row.name === record) ?? null
  return activityPage({
    rows,
    selected: selectedRow?.name ?? null,
    selectedRow,
    detail:
      selectedRow?.kind === 'task'
        ? { kind: 'task', task: selectedRow }
        : selectedRow?.kind === 'run'
          ? {
              kind: 'run',
              run: selectedRow,
              request: approvalRequest,
              source: 'return null;',
              linkedFunctions: [],
              terminal: { kind: 'unavailable' },
            }
          : null,
  })
}

/** Holds every record load until released, so a play can act while one is pending. */
const loads = {
  /** How many record loads have started. */
  count: 0,
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

function RailRoute() {
  const page = useLoaderData<ActivityPage>()
  const navigate = useNavigate()
  const { search } = useLocation()
  return (
    <ActionReviewSession request={page.detail?.kind === 'run' ? page.detail.request : null}>
      <code aria-label="Address">{search}</code>
      <button type="button" onClick={() => navigate(-1)}>
        History back
      </button>
      <button type="button" onClick={() => navigate(1)}>
        History forward
      </button>
      <Activity page={page} />
    </ActionReviewSession>
  )
}

const meta = {
  title: 'Activity/Rail',
  parameters: { layout: 'fullscreen', initialEntry: '/workspaces/finance/activity?tree=1' },
  decorators: [
    (_, context) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: RailRoute,
          HydrateFallback: () => null,
          loader: async ({ request }) => {
            loads.count++
            if (loads.held) await loads.held
            return pageFor(new URL(request.url).searchParams.get('record'))
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

/** The page's parts a play reaches for. */
async function surface(canvasElement: HTMLElement) {
  const canvas = within(canvasElement)
  const screen = within(canvasElement.ownerDocument.body)
  // A modal rail hides the table from the accessibility tree, so find it either way.
  const table = await canvas.findByRole('table', { name: 'Activity records', hidden: true })
  const link = (label: string) => within(table).getByRole('link', { name: label })
  const row = (label: string) => link(label).closest<HTMLElement>('[role="row"]')!
  return {
    canvas,
    screen,
    table,
    link,
    row,
    address: () => canvas.getByLabelText('Address').textContent,
    rail: () => screen.queryByRole('dialog', { name: 'Activity details' }),
    heading: (rail: HTMLElement, name: string) =>
      waitFor(() => expect(within(rail).getByRole('heading', { name, level: 2 })).toBeVisible()),
  }
}

/** Whether the page behind is locked against scrolling, on whichever element scrolls it. */
const scrollLocked = () =>
  [document.documentElement, document.body].some((element) =>
    /hidden|clip/.test(getComputedStyle(element).overflowY),
  )

const settle = () => new Promise((resolve) => setTimeout(resolve, 400))

export const PointerAndHistory: Story = {
  play: async ({ canvasElement, userEvent }) => {
    loads.open()
    const { canvas, screen, table, link, row, address, rail, heading } =
      await surface(canvasElement)
    await userEvent.click(link('Task 1'))
    const opened = await screen.findByRole('dialog', { name: 'Activity details' })
    await heading(opened, 'Task 1')
    // Opening from closed focuses the rail. Beside the page it is not modal.
    await waitFor(() => expect(opened.contains(document.activeElement)).toBe(true))
    await expect(opened).not.toHaveAttribute('aria-modal', 'true')

    // Another record replaces the detail in the same rail; focus stays on its row.
    await userEvent.click(link('Task 2'))
    await heading(opened, 'Task 2')
    await expect(rail()).toBe(opened)
    await expect(link('Task 2')).toHaveFocus()
    // A click on the row outside its link does the same.
    await userEvent.click(within(row('Task 3')).getAllByRole('cell')[0])
    await heading(opened, 'Task 3')
    await expect(rail()).toBe(opened)
    await expect(link('Task 3')).toHaveFocus()

    // The open record does nothing: no load, and no history entry.
    const selected = address()
    const count = loads.count
    await userEvent.click(link('Task 3'))
    await userEvent.click(within(row('Task 3')).getAllByRole('cell')[0])
    // Empty space beside the table leaves the rail open.
    await userEvent.click(canvas.getByRole('heading', { name: 'Your recent activity' }))
    await settle()
    await expect(address()).toBe(selected)
    await expect(loads.count).toBe(count)
    await expect(rail()).toBe(opened)

    // Each new record was one history entry.
    const history = (direction: 'back' | 'forward') =>
      userEvent.click(canvas.getByRole('button', { name: `History ${direction}` }))
    await history('back')
    await heading(opened, 'Task 2')
    await history('back')
    await heading(opened, 'Task 1')
    await expect(rail()).toBe(opened)
    await history('back')
    await waitFor(() => expect(rail()).toBeNull())
    await history('forward')
    const reopened = await screen.findByRole('dialog', { name: 'Activity details' })
    await heading(reopened, 'Task 1')

    // Tab follows the document: from the table into the rail and back.
    await userEvent.click(link('Task 1'))
    await expect(link('Task 1')).toHaveFocus()
    const close = within(reopened).getByRole('button', { name: 'Close details' })
    for (let step = 0; step < 20 && table.contains(document.activeElement); step++)
      await userEvent.tab()
    await expect(close).toHaveFocus()
    await userEvent.tab({ shift: true })
    await expect(table.contains(document.activeElement)).toBe(true)
    await userEvent.tab()
    await expect(close).toHaveFocus()

    // Escape outside the rail is not the rail's; inside, it closes the rail.
    await userEvent.click(link('Task 1'))
    await userEvent.keyboard('{Escape}')
    await settle()
    await expect(rail()).toBe(reopened)
    close.focus()
    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(rail()).toBeNull())
    // Closing returns focus to the record's row in the table.
    await expect(link('Task 1')).toHaveFocus()
    await expect(address()).not.toContain('record=')
  },
}

export const LoadingAndClosing: Story = {
  play: async ({ canvasElement, userEvent }) => {
    const { screen, link, address, rail, heading } = await surface(canvasElement)
    const close = () =>
      userEvent.click(within(rail()!).getByRole('button', { name: 'Close details' }))
    const open = async (label: string) => {
      loads.open()
      await userEvent.click(link(label))
      await heading(await screen.findByRole('dialog', { name: 'Activity details' }), label)
    }
    for (const reduced of [false, true]) {
      await emulateReducedMotion(reduced)
      try {
        // A close while another record loads does not reopen when that load lands.
        await open('Task 1')
        loads.hold()
        await userEvent.click(link('Task 2'))
        await close()
        // The rail closes at once; it does not wait for the address.
        await waitFor(() => expect(rail()).toBeNull())
        loads.open()
        await settle()
        await expect(rail()).toBeNull()
        await expect(address()).not.toContain('record=')

        // A record picked while the rail closes opens, and the close does not take it away.
        await open('Task 1')
        loads.hold()
        await close()
        await userEvent.click(link('Task 3'))
        loads.open()
        await heading(await screen.findByRole('dialog', { name: 'Activity details' }), 'Task 3')
        await settle()
        await heading(rail()!, 'Task 3')
        await expect(address()).toContain('t3')

        // Returning to the open record while another loads stays on it, without a history entry.
        loads.hold()
        await userEvent.click(link('Task 2'))
        await userEvent.click(link('Task 3'))
        loads.open()
        await settle()
        await heading(rail()!, 'Task 3')
        await userEvent.click(within(canvasElement).getByRole('button', { name: 'History back' }))
        await heading(rail()!, 'Task 1')
        await open('Task 3')

        // Picking the closing record again keeps it open.
        loads.hold()
        await close()
        await userEvent.click(link('Task 3'))
        loads.open()
        await settle()
        await heading(rail()!, 'Task 3')
        await expect(address()).toContain('t3')

        // A record picked once the close has already reached the address opens too.
        await close()
        await waitFor(() => expect(address()).not.toContain('record='))
        await userEvent.click(link('Task 2'))
        await heading(await screen.findByRole('dialog', { name: 'Activity details' }), 'Task 2')
        await settle()
        await heading(rail()!, 'Task 2')
        await close()
        await waitFor(() => expect(rail()).toBeNull())
      } finally {
        loads.open()
        await emulateReducedMotion(false)
      }
    }
  },
}

export const UnsentComment: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?tree=1&record=${encodeURIComponent(waiting.name)}`,
  },
  play: async ({ canvasElement, userEvent }) => {
    loads.open()
    const { screen, link, address, rail, heading } = await surface(canvasElement)
    const review = await screen.findByRole('dialog', { name: 'Activity details' })
    const comment = async () => {
      // Testing Library counts a closed disclosure's content as visible, so open it first.
      const summary = within(review).getByText('Add a comment')
      if (!summary.closest('details')!.open) await userEvent.click(summary)
      return within(review).getByRole('textbox', { name: 'Comment (optional)' })
    }
    const confirm = () => screen.findByRole('dialog', { name: 'Discard your comment?' })
    const choose = async (name: 'Keep editing' | 'Discard comment') => {
      await userEvent.click(within(await confirm()).getByRole('button', { name }))
      await waitFor(() =>
        expect(screen.queryByRole('dialog', { name: 'Discard your comment?' })).toBeNull(),
      )
    }
    await userEvent.type(await comment(), 'Check the recipient')
    const reviewing = address()

    // A row click asks first; Keep editing keeps the comment, detail, address and focus.
    await userEvent.click(link('Task 1'))
    const asked = await confirm()
    const layer = (element: Element) =>
      Number(getComputedStyle(element.closest('[role="dialog"]')!).zIndex)
    await expect(layer(asked)).toBeGreaterThan(layer(review))
    await choose('Keep editing')
    await expect(rail()).toBe(review)
    await expect(await comment()).toHaveValue('Check the recipient')
    await expect(address()).toBe(reviewing)
    await waitFor(() => expect(link('Task 1')).toHaveFocus())
    // Discard opens the record in the same rail.
    await userEvent.click(link('Task 1'))
    await choose('Discard comment')
    await heading(review, 'Task 1')
    await expect(rail()).toBe(review)

    // Closing asks too.
    await userEvent.click(link('Reminder run'))
    await heading(review, 'Send invoice reminders')
    await userEvent.type(await comment(), 'Check the amount')
    const closeButton = within(review).getByRole('button', { name: 'Close details' })
    await userEvent.click(closeButton)
    await choose('Keep editing')
    await expect(rail()).toBe(review)
    await expect(await comment()).toHaveValue('Check the amount')
    await waitFor(() => expect(closeButton).toHaveFocus())
    await userEvent.click(closeButton)
    await choose('Discard comment')
    await waitFor(() => expect(rail()).toBeNull())
    await expect(address()).not.toContain('record=')
  },
}

export const Mobile: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?tree=1&record=${encodeURIComponent(tasks[0].name)}`,
  },
  play: async ({ canvasElement, userEvent }) => {
    loads.open()
    if (!(await resizeViewport(390, 800))) return
    const { canvas, table, link, rail, heading } = await surface(canvasElement)
    const opened = await within(document.body).findByRole('dialog', { name: 'Activity details' })
    await heading(opened, 'Task 1')
    // At 640px or less the rail is a full-screen modal: the page is hidden and locked.
    await waitFor(() => expect(opened.contains(document.activeElement)).toBe(true))
    await waitFor(() => expect(within(table).queryByRole('link', { name: 'Task 2' })).toBeNull())
    await expect(opened.getBoundingClientRect().width).toBe(window.innerWidth)
    await expect(scrollLocked()).toBe(true)

    // Wider, the same record stays open beside the page, and focus stays put.
    const focused = document.activeElement
    await resizeViewport(1200, 900)
    await waitFor(() =>
      expect(within(table).queryByRole('link', { name: 'Task 2' })).not.toBeNull(),
    )
    await expect(rail()).toBe(opened)
    await expect(document.activeElement).toBe(focused)
    await expect(scrollLocked()).toBe(false)
    await userEvent.click(link('Task 2'))
    await heading(opened, 'Task 2')

    // Narrow again, the rail turns modal and takes focus.
    await resizeViewport(390, 800)
    await waitFor(() => expect(opened.contains(document.activeElement)).toBe(true))
    await expect(rail()).toBe(opened)
    await heading(opened, 'Task 2')
    await waitFor(() =>
      expect(canvas.queryByRole('heading', { name: 'Your recent activity' })).toBeNull(),
    )
    await userEvent.click(within(opened).getByRole('button', { name: 'Close details' }))
    await waitFor(() => expect(rail()).toBeNull())
    await expect(link('Task 2')).toHaveFocus()
  },
}
