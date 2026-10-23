import type { Meta, StoryObj } from '@storybook/react'
import {
  createRoutesStub,
  useLoaderData,
  useLocation,
  useNavigate,
  useNavigation,
} from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { activityPage, approvalContext, approvalRequest, run, workspace } from './fixtures'
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
              // The program that makes the call this review decides.
              source: approvalContext.source,
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
  /** How many record loads have not finished yet. */
  inFlight: 0,
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
  const { state } = useNavigation()
  return (
    <ActionReviewSession request={page.detail?.kind === 'run' ? page.detail.request : null}>
      <code aria-label="Address">{search}</code>
      <output aria-label="Navigation">{state}</output>
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
            loads.inFlight++
            try {
              if (loads.held) await loads.held
              return pageFor(new URL(request.url).searchParams.get('record'))
            } finally {
              loads.inFlight--
            }
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
type PlayUserEvent = Parameters<NonNullable<Story['play']>>[0]['userEvent']

/**
 * Opens the comment disclosure in the rail if it is closed and returns its textbox.
 * Testing Library counts a closed disclosure's content as visible, so open it first.
 */
async function openCommentBox(rail: HTMLElement, userEvent: PlayUserEvent) {
  const summary = within(rail).getByText('Add a comment')
  if (!summary.closest('details')!.open) await userEvent.click(summary)
  return within(rail).getByRole('textbox', { name: 'Comment (optional)' })
}

/** The page's parts a play reaches for. */
async function surface(canvasElement: HTMLElement) {
  const canvas = within(canvasElement)
  const screen = within(canvasElement.ownerDocument.body)
  // A modal rail hides the table from the accessibility tree, so find it either way.
  const table = await canvas.findByRole('table', { name: 'Activity records', hidden: true })
  const link = (label: string) => within(table).getByRole('link', { name: label })
  const row = (label: string) => link(label).closest<HTMLElement>('[role="row"]')!
  return {
    /** Where focus rests, and the record the table's cursor row shows: "table on Task 1". */
    focus: () => {
      const focused = document.activeElement
      const where =
        focused === table
          ? 'table'
          : (focused?.getAttribute('aria-label') ?? focused?.textContent ?? 'nothing')
      const cursor = table.querySelector('[data-active] a[href]')?.textContent ?? 'no row'
      return `${where} on ${cursor}`
    },
    canvas,
    screen,
    table,
    link,
    row,
    address: () => canvas.getByLabelText('Address').textContent,
    rail: () => screen.queryByRole('dialog', { name: 'Activity details' }),
    // A record heads the rail with its name; an approval review heads it with its call.
    heading: (rail: HTMLElement, name: string) =>
      waitFor(() => expect(within(rail).getByRole('heading', { name })).toBeVisible()),
    /** Waits until the rail shows `name`. A rail that closed and reopened is a new element. */
    shows: (name: string) =>
      waitFor(() =>
        expect(
          within(screen.getByRole('dialog', { name: 'Activity details' })).getByRole('heading', {
            name,
          }),
        ).toBeVisible(),
      ),
  }
}

/**
 * Waits until every record load has finished and the router is idle, then one more
 * frame, so a rail that a late load would reopen has rendered by the next assertion.
 */
async function quiet(canvasElement: HTMLElement) {
  await waitFor(() => {
    expect(loads.inFlight).toBe(0)
    expect(within(canvasElement).getByLabelText('Navigation')).toHaveTextContent('idle')
  })
  await new Promise((resolve) => requestAnimationFrame(resolve))
}

export const PointerAndHistory: Story = {
  play: async ({ canvasElement, userEvent }) => {
    loads.open()
    const { canvas, screen, table, link, row, address, rail, heading, focus } =
      await surface(canvasElement)
    // A rail opened by pointer takes focus a frame after it shows. Let that land
    // before the next click, or it pulls focus from the table after that click.
    const focused = (dialog: HTMLElement) =>
      waitFor(() => expect(dialog).toContainElement(document.activeElement as HTMLElement))
    await userEvent.click(link('Task 1'))
    const opened = await screen.findByRole('dialog', { name: 'Activity details' })
    await heading(opened, 'Task 1')
    await focused(opened)

    // Another record replaces the detail in the same rail; focus rests on the table,
    // whose cursor row shows it.
    await userEvent.click(link('Task 2'))
    await heading(opened, 'Task 2')
    await expect(rail()).toBe(opened)
    await waitFor(() => expect(focus()).toBe('table on Task 2'))
    // A click on the row outside its link does the same.
    await userEvent.click(within(row('Task 3')).getAllByRole('cell')[0])
    await heading(opened, 'Task 3')
    await expect(rail()).toBe(opened)
    await waitFor(() => expect(focus()).toBe('table on Task 3'))

    // The open record does nothing: no load, and no history entry.
    const selected = address()
    const count = loads.count
    await userEvent.click(link('Task 3'))
    await userEvent.click(within(row('Task 3')).getAllByRole('cell')[0])
    // Empty space beside the table leaves the rail open.
    await userEvent.click(canvas.getByRole('heading', { name: 'Your recent activity' }))
    await quiet(canvasElement)
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
    await focused(reopened)

    // Tab follows the document: from the table into the rail and back.
    await userEvent.click(link('Task 1'))
    await waitFor(() => expect(focus()).toBe('table on Task 1'))
    const close = within(reopened).getByRole('button', { name: 'Close details' })
    // The rail's first control is its live toggle.
    const follow = within(reopened).getByRole('button', { name: 'Follow this task' })
    for (let step = 0; step < 20 && table.contains(document.activeElement); step++)
      await userEvent.tab()
    await waitFor(() => expect(follow).toHaveFocus())
    await userEvent.tab({ shift: true })
    await waitFor(() => expect(table.contains(document.activeElement)).toBe(true))
    await userEvent.tab()
    await waitFor(() => expect(follow).toHaveFocus())

    // Escape in the rail closes it.
    close.focus()
    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(rail()).toBeNull())
    // Closing returns focus to the table, the one Tab stop of its records.
    await waitFor(() => expect(table).toHaveFocus())
    await expect(address()).not.toContain('record=')
  },
}

export const LoadingAndClosing: Story = {
  play: async ({ canvasElement, userEvent }) => {
    const { link, address, rail, shows } = await surface(canvasElement)
    const close = () =>
      userEvent.click(within(rail()!).getByRole('button', { name: 'Close details' }))
    const open = async (label: string) => {
      loads.open()
      await userEvent.click(link(label))
      await shows(label)
    }
    for (const reduced of [false, true]) {
      // Storybook's own UI cannot emulate reduced motion, so it plays normal motion only.
      if (!(await emulateReducedMotion(reduced)) && reduced) break
      try {
        // A close while another record loads does not reopen when that load lands.
        await open('Task 1')
        loads.hold()
        await userEvent.click(link('Task 2'))
        await close()
        // The rail closes at once; it does not wait for the address.
        await waitFor(() => expect(rail()).toBeNull())
        loads.open()
        await quiet(canvasElement)
        await expect(rail()).toBeNull()
        await expect(address()).not.toContain('record=')

        // A record picked while the rail closes opens, and the close does not take it away.
        await open('Task 1')
        loads.hold()
        await close()
        await userEvent.click(link('Task 3'))
        loads.open()
        await shows('Task 3')
        await quiet(canvasElement)
        await shows('Task 3')
        await expect(address()).toContain('t3')

        // Returning to the open record while another loads stays on it, without a history entry.
        loads.hold()
        await userEvent.click(link('Task 2'))
        await userEvent.click(link('Task 3'))
        loads.open()
        await quiet(canvasElement)
        await shows('Task 3')
        await userEvent.click(within(canvasElement).getByRole('button', { name: 'History back' }))
        await shows('Task 1')
        await open('Task 3')

        // Picking the closing record again keeps it open.
        loads.hold()
        await close()
        await userEvent.click(link('Task 3'))
        loads.open()
        await quiet(canvasElement)
        await shows('Task 3')
        await expect(address()).toContain('t3')

        // A record picked once the close has already reached the address opens too.
        await close()
        await waitFor(() => expect(address()).not.toContain('record='))
        await userEvent.click(link('Task 2'))
        await shows('Task 2')
        await quiet(canvasElement)
        await shows('Task 2')
        await close()
        await waitFor(() => expect(rail()).toBeNull())
      } finally {
        loads.open()
        await emulateReducedMotion(false)
      }
    }
  },
}

/**
 * Safari does not focus a link on a mouse click. A DOM `click()` does not move focus
 * either, so it stands in for that click here. WebKit itself does not run in this suite.
 */
export const LinkClickFocus: Story = {
  play: async ({ canvasElement }) => {
    loads.open()
    const { screen, link, shows, focus } = await surface(canvasElement)
    link('Task 1').click()
    await shows('Task 1')
    const opened = screen.getByRole('dialog', { name: 'Activity details' })
    // The rail takes focus as it opens; let that land before moving it.
    await waitFor(() => expect(opened.contains(document.activeElement)).toBe(true))
    within(opened).getByRole('button', { name: 'Close details' }).focus()
    // Picking another record leaves focus on the table, on its row, whatever the browser does.
    link('Task 2').click()
    await shows('Task 2')
    await waitFor(() => expect(focus()).toBe('table on Task 2'))
  },
}

export const HistoryWhileLoading: Story = {
  play: async ({ canvasElement, userEvent }) => {
    loads.open()
    const { canvas, link, address, rail, shows } = await surface(canvasElement)
    const history = (direction: 'back' | 'forward') =>
      userEvent.click(canvas.getByRole('button', { name: `History ${direction}` }))
    await userEvent.click(link('Task 1'))
    await shows('Task 1')
    await userEvent.click(link('Task 2'))
    await shows('Task 2')

    // Back is under way when you pick the record still shown. The pick is a new
    // entry after Task 1, so the entries before it stay.
    loads.hold()
    await history('back')
    await userEvent.click(link('Task 2'))
    loads.open()
    // The address and rail already show the record; wait for the navigation to land.
    await quiet(canvasElement)
    await waitFor(() => expect(address()).toContain('t2'))
    await shows('Task 2')
    await history('back')
    await shows('Task 1')

    // The same with Forward: the pick follows the entry Forward reached.
    loads.hold()
    await history('forward')
    await userEvent.click(link('Task 1'))
    loads.open()
    await quiet(canvasElement)
    await waitFor(() => expect(address()).toContain('t1'))
    await shows('Task 1')
    await history('back')
    await shows('Task 2')
    await history('back')
    await shows('Task 1')
    await history('back')
    await waitFor(() => expect(rail()).toBeNull())

    // Back interrupts a pick of the record Back reaches; a later pick is still a new entry.
    await userEvent.click(link('Task 1'))
    await shows('Task 1')
    await userEvent.click(link('Task 2'))
    await shows('Task 2')
    loads.hold()
    await userEvent.click(link('Task 1'))
    await history('back')
    await userEvent.click(link('Task 2'))
    loads.open()
    await quiet(canvasElement)
    await waitFor(() => expect(address()).toContain('t2'))
    await shows('Task 2')
    await history('back')
    await shows('Task 1')

    // A pick that interrupts Back, then a pick of the record still shown: Back already
    // moved through history, so the second pick is a new entry too.
    await userEvent.click(link('Task 2'))
    await shows('Task 2')
    loads.hold()
    await history('back')
    await userEvent.click(link('Task 3'))
    await userEvent.click(link('Task 2'))
    loads.open()
    await quiet(canvasElement)
    await waitFor(() => expect(address()).toContain('t2'))
    await shows('Task 2')
    await history('back')
    await shows('Task 1')
  },
}

/**
 * A record picked while its own close loads cancels the close. The close never
 * reached history, so Back from the record returns to the list.
 */
export const PickWhileClosing: Story = {
  play: async ({ canvasElement, userEvent }) => {
    loads.open()
    const { canvas, screen, link, address, rail, shows } = await surface(canvasElement)
    const back = async () => {
      await userEvent.click(canvas.getByRole('button', { name: 'History back' }))
      await quiet(canvasElement)
    }
    const loading = () =>
      waitFor(() =>
        expect(within(canvasElement).getByLabelText('Navigation')).toHaveTextContent('loading'),
      )
    const closeButton = () => within(rail()!).getByRole('button', { name: 'Close details' })
    const discardPrompt = () => screen.findByRole('dialog', { name: 'Discard your comment?' })
    const typeComment = async (text: string) =>
      userEvent.type(await openCommentBox(rail()!, userEvent), text)
    const pickBackToList = async (label: string, heading: string) => {
      await userEvent.click(link(label))
      loads.open()
      await quiet(canvasElement)
      await shows(heading)
      await back()
      await waitFor(() => expect(rail()).toBeNull())
      await expect(address()).not.toContain('record=')
    }

    // Without a comment, the close starts at once.
    await userEvent.click(link('Task 1'))
    await shows('Task 1')
    await quiet(canvasElement)
    loads.hold()
    await userEvent.click(closeButton())
    await loading()
    await pickBackToList('Task 1', 'Task 1')

    // With an unsent comment, the close starts once you discard it.
    await userEvent.click(link('Reminder run'))
    await shows(approvalRequest.callPath)
    await quiet(canvasElement)
    await typeComment('Check the recipient')
    loads.hold()
    await userEvent.click(closeButton())
    await userEvent.click(
      within(await discardPrompt()).getByRole('button', { name: 'Discard comment' }),
    )
    await loading()
    await pickBackToList('Reminder run', approvalRequest.callPath)

    // Keep editing cancels the close; a later discarded close behaves the same.
    await userEvent.click(link('Reminder run'))
    await shows(approvalRequest.callPath)
    await quiet(canvasElement)
    await typeComment('Check the amount')
    await userEvent.click(closeButton())
    await userEvent.click(
      within(await discardPrompt()).getByRole('button', { name: 'Keep editing' }),
    )
    await quiet(canvasElement)
    await expect(address()).toContain('runs')
    loads.hold()
    await userEvent.click(closeButton())
    await userEvent.click(
      within(await discardPrompt()).getByRole('button', { name: 'Discard comment' }),
    )
    await loading()
    await pickBackToList('Reminder run', approvalRequest.callPath)
  },
}

export const UnsentComment: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?tree=1&record=${encodeURIComponent(waiting.name)}`,
  },
  play: async ({ canvasElement, userEvent }) => {
    loads.open()
    const { screen, link, address, rail, heading, focus } = await surface(canvasElement)
    const review = await screen.findByRole('dialog', { name: 'Activity details' })
    const comment = () => openCommentBox(review, userEvent)
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
    // Focus is back on the table, whose cursor row is still the record under review.
    await waitFor(() => expect(focus()).toBe('table on Reminder run'))
    // Discard opens the record in the same rail.
    await userEvent.click(link('Task 1'))
    await choose('Discard comment')
    await heading(review, 'Task 1')
    await expect(rail()).toBe(review)

    // Closing asks too.
    await userEvent.click(link('Reminder run'))
    await heading(review, approvalRequest.callPath)
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
    // Storybook's own UI cannot resize its viewport, so this play needs the test runner.
    if (!(await resizeViewport(390, 800))) return
    const { canvas, table, link, rail, heading } = await surface(canvasElement)
    const opened = await within(document.body).findByRole('dialog', { name: 'Activity details' })
    await heading(opened, 'Task 1')
    // At 640px or less the rail is a full-screen modal over the table.
    await waitFor(() => expect(within(table).queryByRole('link', { name: 'Task 2' })).toBeNull())
    await expect(opened.getBoundingClientRect().width).toBeCloseTo(window.innerWidth, 0)

    // Wider, the same record stays open beside the table, which is usable again.
    await resizeViewport(1200, 900)
    await waitFor(() =>
      expect(within(table).queryByRole('link', { name: 'Task 2' })).not.toBeNull(),
    )
    await expect(rail()).toBe(opened)
    await userEvent.click(link('Task 2'))
    await heading(opened, 'Task 2')

    // Narrow again, the rail keeps the record it switched to.
    await resizeViewport(390, 800)
    await expect(rail()).toBe(opened)
    await heading(opened, 'Task 2')
    await waitFor(() =>
      expect(canvas.queryByRole('heading', { name: 'Your recent activity' })).toBeNull(),
    )
    await userEvent.click(within(opened).getByRole('button', { name: 'Close details' }))
    await waitFor(() => expect(rail()).toBeNull())
    await waitFor(() => expect(table).toHaveFocus())
  },
}
