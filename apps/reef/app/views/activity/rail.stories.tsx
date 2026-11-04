import type { Meta, StoryObj } from '@storybook/react-vite'
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
import { activityPage, approvalContext, approvalRequest, run, runRow, taskRow } from './fixtures'
import { leavesActivityReview } from './navigation'
import type { ActivityPage, ActivityRow, RunDetail } from './types'
import { resizeViewport } from '../../../.storybook/browser'

const at = '2026-09-18T14:02:00.000Z'
const goal = taskRow('collect', 'Collect overdue invoices')
const runs = [1, 2, 3].map((index) =>
  runRow(`r${index}`, goal.name, { intent: `Run ${index}`, startedAt: at, state: 'succeeded' }),
)
const waiting: ActivityRow & { kind: 'run' } = {
  ...run,
  task: goal.name,
  intent: 'Reminder run',
  agent: '',
  startedAt: at,
}
const records = [...runs, waiting]

/** The page the loader returns for the record the URL selects, with the Task's timeline open. */
function pageFor(record: string | null): ActivityPage {
  const selectedRow = records.find((row) => row.name === record) ?? null
  const detail: RunDetail | null =
    selectedRow === waiting
      ? { ...approvalContext, run: waiting, request: approvalRequest }
      : selectedRow && {
          kind: 'run',
          run: selectedRow,
          request: null,
          source: 'return 1',
          linkedFunctions: [],
          terminal: { kind: 'unavailable' },
        }
  return activityPage({
    rows: [goal],
    trace: { root: goal, children: { [goal.name]: records }, complete: true, unavailable: [] },
    selected: selectedRow?.name ?? null,
    selectedRow,
    detail,
  })
}

function RailRoute() {
  const page = useLoaderData<ActivityPage>()
  const navigate = useNavigate()
  const { search } = useLocation()
  const { state } = useNavigation()
  const request = page.detail?.kind === 'run' ? page.detail.request : null
  return (
    <ActionReviewSession request={request} leaves={leavesActivityReview(request)}>
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
  parameters: {
    layout: 'fullscreen',
    initialEntry: `/workspaces/finance/activity?root=${encodeURIComponent(goal.name)}`,
  },
  decorators: [
    (_, context) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: RailRoute,
          HydrateFallback: () => null,
          loader: ({ request }) => pageFor(new URL(request.url).searchParams.get('record')),
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
  // A modal rail hides the timeline from the accessibility tree, so find it either way.
  const timeline = await canvas.findByRole('table', { name: 'Timeline records', hidden: true })
  const link = (label: string) => within(timeline).getByRole('link', { name: label })
  return {
    canvas,
    screen,
    timeline,
    link,
    address: () => canvas.getByLabelText('Address').textContent,
    rail: () => screen.queryByRole('dialog', { name: /^(Activity details|Review action)$/ }),
    /** Waits until the open rail heads its detail with `name`. */
    shows: (name: string) =>
      waitFor(() =>
        expect(
          within(
            screen.getByRole('dialog', { name: /^(Activity details|Review action)$/ }),
          ).getByRole('heading', { name }),
        ).toBeVisible(),
      ),
  }
}

/** A row of the timeline opens its record's detail; another row replaces it in place. */
export const PointerAndHistory: Story = {
  play: async ({ canvasElement, userEvent }) => {
    const { canvas, link, address, rail, shows } = await surface(canvasElement)
    await userEvent.click(link('Run 1'))
    await shows('Run 1')
    const opened = rail()!
    await waitFor(() => expect(opened).toContainElement(document.activeElement as HTMLElement))

    await userEvent.click(link('Run 2'))
    await shows('Run 2')
    await expect(rail()).toBe(opened)

    // Each record was one history entry; Back past the first closes the rail.
    const history = (direction: 'back' | 'forward') =>
      userEvent.click(canvas.getByRole('button', { name: `History ${direction}` }))
    await history('back')
    await shows('Run 1')
    await history('back')
    await waitFor(() => expect(rail()).toBeNull())
    await history('forward')
    await shows('Run 1')

    // Escape closes the rail and leaves the timeline's root open.
    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(rail()).toBeNull())
    await expect(address()).not.toContain('record=')
    await expect(address()).toContain('root=')
  },
}

/** Leaving a review with an unsent comment asks first, from a row and from the close control. */
export const UnsentComment: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?root=${encodeURIComponent(goal.name)}&record=${encodeURIComponent(waiting.name)}`,
  },
  play: async ({ canvasElement, userEvent }) => {
    const { screen, link, address, rail, shows } = await surface(canvasElement)
    const review = await screen.findByRole('dialog', { name: /^(Activity details|Review action)$/ })
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

    // A row click asks first; Keep editing keeps the comment, the review and the address.
    await userEvent.click(link('Run 1'))
    await choose('Keep editing')
    await expect(rail()).toBe(review)
    await expect(await comment()).toHaveValue('Check the recipient')
    await expect(address()).toBe(reviewing)
    // Discard opens the record.
    await userEvent.click(link('Run 1'))
    await choose('Discard comment')
    await shows('Run 1')

    // Closing asks too.
    await userEvent.click(link('Reminder run'))
    await shows(approvalRequest.callPath)
    const reopened = rail()!
    await userEvent.type(await openCommentBox(reopened, userEvent), 'Check the amount')
    const closeButton = within(reopened).getByRole('button', { name: 'Close details' })
    await userEvent.click(closeButton)
    await choose('Keep editing')
    await expect(rail()).toBe(reopened)
    await userEvent.click(closeButton)
    await choose('Discard comment')
    await waitFor(() => expect(rail()).toBeNull())
    await expect(address()).not.toContain('record=')
  },
}

/** At 640px or less the rail is a full-screen modal over the page. */
export const Mobile: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?root=${encodeURIComponent(goal.name)}&record=${encodeURIComponent(runs[0]!.name)}`,
  },
  play: async ({ canvasElement, userEvent }) => {
    // Storybook's own UI cannot resize its viewport, so this play needs the test runner.
    if (!(await resizeViewport(390, 800))) return
    const { rail, shows } = await surface(canvasElement)
    await shows('Run 1')
    const opened = rail()!
    await expect(opened.getBoundingClientRect().width).toBeCloseTo(window.innerWidth, 0)
    await userEvent.click(within(opened).getByRole('button', { name: 'Close details' }))
    await waitFor(() => expect(rail()).toBeNull())
    await resizeViewport(1200, 900)
  },
}

/** The box of an element as it renders now. */
const box = (element: Element) => element.getBoundingClientRect()

/**
 * At 1200px or more the rail docks beside the timeline, which shrinks to make room and gets
 * its width back when the rail closes.
 */
export const Docked: Story = {
  play: async ({ canvasElement, userEvent }) => {
    // Storybook's own UI cannot resize its viewport, so this play needs the test runner.
    if (!(await resizeViewport(1440, 900))) return
    const { timeline, link, rail, shows } = await surface(canvasElement)
    const full = box(timeline).width
    await userEvent.click(link('Run 1'))
    await shows('Run 1')
    const opened = rail()!
    await waitFor(() => {
      expect(box(timeline).right).toBeLessThanOrEqual(box(opened).left)
      expect(box(opened).right).toBeLessThanOrEqual(window.innerWidth)
    })
    await expect(box(timeline).width).toBeLessThan(full)
    // Every record stays in reach beside the rail.
    await userEvent.click(link('Run 3'))
    await shows('Run 3')
    await expect(rail()).toBe(opened)
    await userEvent.click(within(opened).getByRole('button', { name: 'Close details' }))
    await waitFor(() => expect(rail()).toBeNull())
    await waitFor(() => expect(box(timeline).width).toBeCloseTo(full, 0))
  },
}
