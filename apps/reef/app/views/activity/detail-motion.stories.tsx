import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub, useLoaderData, useLocation, useNavigate } from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { ActionReviewSession } from '../actions/review'
import { Activity } from './activity'
import { activityPage, invocationRow, runRow } from './fixtures'
import type { ActivityPage, ActivityRow, RunDetail, RunEvent } from './types'
import {
  emulateReducedMotion,
  inTestBrowser,
  press,
  recordAnimations,
} from '../../../.storybook/browser'

// The selected record's slide between records of one Run, in Chromium: its
// direction, what it leaves alone, and when it replaces the record at once.

type Tool = Extract<ActivityRow, { kind: 'invocation' }>
const reminders = runRow('reminders', '', { actor: 'Reminder run', intent: 'Send reminders' })
const audit = runRow('audit', '', { actor: 'Audit run', intent: 'Audit invoices' })
const programSource = Array.from({ length: 80 }, (_, line) => `// line ${line + 1}`).join('\n')

const tool = (run: string, id: string, callPath: string, position?: number): Tool =>
  invocationRow(run, id, {
    tool: callPath.split('.').at(-1)!,
    callPath,
    position,
    sourceLocation: { document: { kind: 'program' }, line: 2 + (position ?? 0), column: 1 },
  })
const list = tool(reminders.name, 'list', 'billing.list', 0)
const send = tool(reminders.name, 'send', 'mail.send', 1)
const check = tool(reminders.name, 'check', 'mail.check', 2)
const logged: Extract<ActivityRow, { kind: 'output' }> = {
  kind: 'output',
  name: `${reminders.name}/events/3`,
  workspace: reminders.workspace,
  run: reminders.name,
  label: 'log #3',
  startedAt: reminders.startedAt,
  eventKind: 'program.logged',
  agent: '',
  position: 3,
}
const archive = tool(reminders.name, 'archive', 'mail.archive', 4)
const notify = tool(reminders.name, 'notify', 'mail.notify', 5)
const emitted: Extract<ActivityRow, { kind: 'output' }> = {
  ...logged,
  name: `${reminders.name}/events/6`,
  label: 'emission #6',
  eventKind: 'program.emitted',
  position: 6,
}
// Lifecycle records: a Tool call ending, and a request to cancel the Run.
const ended: Extract<ActivityRow, { kind: 'output' }> = {
  ...logged,
  name: `${reminders.name}/events/7`,
  label: 'ended #7',
  eventKind: 'invocation.ended',
  position: 7,
}
const cancelling: Extract<ActivityRow, { kind: 'output' }> = {
  ...logged,
  name: `${reminders.name}/events/8`,
  label: 'cancel #8',
  eventKind: 'run.cancellation_requested',
  position: 8,
}
// The Run's tenth record: Coral holds it, but the loaded window ends before it.
const unloaded = tool(reminders.name, 'unloaded', 'mail.unloaded')
const audited = tool(audit.name, 'audited', 'audit.check', 0)

const events: Record<string, RunEvent> = {
  [logged.name]: {
    kind: 'program.logged',
    sequence: '3',
    occurredAt: reminders.startedAt,
    level: 'info',
    message: 'checked',
  },
  [emitted.name]: {
    kind: 'program.emitted',
    sequence: '6',
    occurredAt: reminders.startedAt,
    value: '{"sent":2}',
  },
  [ended.name]: {
    kind: 'invocation.ended',
    sequence: '7',
    occurredAt: reminders.startedAt,
    invocation: notify.name,
    outcome: 'upstream_outcome',
  },
  [cancelling.name]: {
    kind: 'run.cancellation_requested',
    sequence: '8',
    occurredAt: reminders.startedAt,
    actor: 'you',
    actorKind: 'user',
  },
}
const branchRows = [list, send, check, logged, archive, notify, emitted, ended, cancelling]
const records: ActivityRow[] = [reminders, audit, ...branchRows, unloaded, audited]

function runDetail(run: Extract<ActivityRow, { kind: 'run' }>): RunDetail {
  return {
    kind: 'run',
    run,
    request: null,
    requestUnavailable: false,
    source: programSource,
    linkedFunctions: [],
    terminal: { kind: 'unavailable' },
  }
}

function pageFor(url: URL): ActivityPage {
  const record = url.searchParams.get('record')
  const selectedRow = records.find((row) => row.name === record) ?? null
  const contextOf = (run: string) => runDetail(run === audit.name ? audit : reminders)
  return activityPage({
    rows: [reminders, audit],
    expanded: [reminders.name, audit.name],
    branches: {
      [reminders.name]: { rows: branchRows, offset: 0, total: '10', next: '', unavailable: false },
      [audit.name]: { rows: [audited], offset: 0, total: '1', next: '', unavailable: false },
    },
    selected: selectedRow?.name ?? null,
    selectedRow,
    detail:
      selectedRow?.kind === 'run'
        ? runDetail(selectedRow)
        : selectedRow?.kind === 'invocation'
          ? {
              kind: 'invocation',
              invocation: selectedRow,
              // A tall argument, so the selected record scrolls.
              arguments: JSON.stringify(
                Object.fromEntries(Array.from({ length: 80 }, (_, key) => [`key${key}`, key])),
                null,
                2,
              ),
              result: { kind: 'upstream_outcome', value: '{}' },
              requests: [],
              request: null,
              requestUnavailable: false,
              context: contextOf(selectedRow.run),
            }
          : selectedRow?.kind === 'output'
            ? {
                kind: 'output',
                run: selectedRow.run,
                event: events[selectedRow.name]!,
                context: contextOf(selectedRow.run),
              }
            : null,
  })
}

function MotionRoute() {
  const page = useLoaderData<ActivityPage>()
  const navigate = useNavigate()
  const { search } = useLocation()
  // A record you reach by its address alone, such as from a link elsewhere.
  const open = (row: ActivityRow) => {
    const next = new URLSearchParams(search)
    next.set('record', row.name)
    void navigate(`?${next}`)
  }
  return (
    <ActionReviewSession request={null}>
      <button type="button" onClick={() => navigate(-1)}>
        History back
      </button>
      <button type="button" onClick={() => open(unloaded)}>
        Open unloaded tool
      </button>
      <Activity page={page} windowKey="motion" />
    </ActionReviewSession>
  )
}

const expand = `tree=1&expand=${reminders.name}&expand=${audit.name}`
const meta = {
  title: 'Activity/Detail motion',
  parameters: {
    layout: 'fullscreen',
    chromatic: { disableSnapshot: true },
    initialEntry: `/workspaces/finance/activity?${expand}&record=${send.name}`,
  },
  decorators: [
    (_, context) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: MotionRoute,
          HydrateFallback: () => null,
          loader: ({ request }) => pageFor(new URL(request.url)),
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
type Play = Parameters<NonNullable<Story['play']>>[0]

/** What a slide moves: the record that enters and the one that leaves, by their headings. */
interface Slide {
  entering?: { heading: string; transforms: string[] }
  leaving?: { heading: string; transforms: string[] }
  /** Animations elsewhere in the drawer, such as in the source. */
  elsewhere: number
}

async function surface({ canvasElement, userEvent }: Play) {
  const canvas = within(canvasElement)
  const body = within(canvasElement.ownerDocument.body)
  const table = await canvas.findByRole('table', { name: 'Activity records', hidden: true })
  const rail = () => body.getByRole('dialog', { name: 'Activity details' })
  const selected = () => within(rail()).getByRole('region', { name: 'Selected record' })
  /**
   * Each animated card as it starts to move: whether it is in the selected record's pane,
   * whether it is the leaving copy, and the record it names. The leaving copy empties
   * once its slide ends, so a play that looks later still sees what moved.
   */
  const recorder = recordAnimations((target) => {
    const region = rail().querySelector('[aria-label="Selected record"]')
    return {
      inRegion: region?.contains(target) ?? false,
      leaving: target.closest('[aria-hidden="true"]') !== null,
      // The leaving copy is hidden from assistive technology, so look past that.
      heading:
        within(target as HTMLElement).queryByRole('heading', { level: 2, hidden: true })
          ?.textContent ?? '',
    }
  })
  /** The layer that holds the leaving record's copy while it slides. */
  const leavingCopy = () => selected().querySelector(':scope > [aria-hidden="true"]')!
  /** The motion the last selection started. */
  const slide = (): Slide => {
    const inRegion = recorder.started.filter(({ note }) => note.inRegion)
    const move = (leaving: boolean) => {
      const found = inRegion.find(({ note }) => note.leaving === leaving)
      return found && { heading: found.note.heading, transforms: found.transforms }
    }
    return {
      entering: move(false),
      leaving: move(true),
      elsewhere: recorder.started.length - inRegion.length,
    }
  }
  /** Waits until the rail shows `heading`, then returns the slide that got it there. */
  const shows = async (heading: string) => {
    await waitFor(() =>
      expect(within(selected()).getByRole('heading', { name: heading, level: 2 })).toBeVisible(),
    )
    const started = slide()
    // The next step starts from rest, unless the play holds this slide on purpose.
    await Promise.all(
      recorder.started
        .filter(({ animation }) => animation.playState === 'running')
        .map(({ animation }) => animation.finished.catch(() => {})),
    )
    recorder.clear()
    return started
  }
  return {
    canvas,
    table,
    rail,
    selected,
    leavingCopy,
    recorder,
    shows,
    select: (label: string) => userEvent.click(within(table).getByRole('link', { name: label })),
    press: (name: string) => userEvent.click(canvas.getByRole('button', { name })),
  }
}

const down = (from: string, to: string): Slide => ({
  entering: { heading: to, transforms: ['translateY(100%)', 'translateY(0)'] },
  leaving: { heading: from, transforms: ['translateY(0)', 'translateY(-100%)'] },
  elsewhere: 0,
})
const up = (from: string, to: string): Slide => ({
  entering: { heading: to, transforms: ['translateY(-100%)', 'translateY(0)'] },
  leaving: { heading: from, transforms: ['translateY(0)', 'translateY(100%)'] },
  elsewhere: 0,
})
const instant: Slide = { entering: undefined, leaving: undefined, elsewhere: 0 }

/** A later tool enters from below and an earlier one from above, by click, key or Back. */
export const SlidesInRunOrder: Story = {
  play: async (context) => {
    if (!(await inTestBrowser())) return
    const { rail, recorder, shows, select, press: button } = await surface(context)
    try {
      await shows('mail.send')
      await select('mail.check')
      await expect(await shows('mail.check')).toEqual(down('mail.send', 'mail.check'))
      // Back returns to the earlier tool, so it enters from above.
      await button('History back')
      await expect(await shows('mail.send')).toEqual(up('mail.check', 'mail.send'))

      // ↓ and ↑ on the rail step by Run order, and focus stays on the rail.
      rail().focus()
      await press('{ArrowDown}')
      await expect(await shows('mail.check')).toEqual(down('mail.send', 'mail.check'))
      await press('{ArrowUp}')
      await expect(await shows('mail.send')).toEqual(up('mail.check', 'mail.send'))
      await expect(rail()).toHaveFocus()

      // A jump past several records is one slide of the same distance, straight to it.
      await select('mail.notify')
      await expect(await shows('mail.notify')).toEqual(down('mail.send', 'mail.notify'))
      await select('billing.list')
      await expect(await shows('billing.list')).toEqual(up('mail.notify', 'billing.list'))
    } finally {
      recorder.stop()
    }
  },
}

/**
 * A reversal mid-slide ends that slide at once and slides straight back from where the
 * card was, with no queue.
 */
export const ReversesMidSlide: Story = {
  play: async (context) => {
    if (!(await inTestBrowser())) return
    const { leavingCopy, recorder, shows, select } = await surface(context)
    try {
      await shows('mail.send')
      await select('mail.check')
      await waitFor(() => expect(recorder.started).toHaveLength(2))
      // Hold the slide halfway, so the next selection lands while it plays.
      const first = recorder.started.map(({ animation }) => animation)
      for (const animation of first) {
        animation.pause()
        animation.currentTime = 100
      }
      recorder.clear()
      await select('mail.send')
      await waitFor(() => expect(recorder.started).toHaveLength(2))
      const second = recorder.started.map(({ animation }) => animation)
      for (const animation of second) animation.pause()
      const reversal = await shows('mail.send')
      // The card that leaves starts halfway down the pane, where the held slide left it.
      const [from, to] = reversal.leaving!.transforms
      await expect(from).toMatch(/^translateY\(\d+(\.\d+)?px\)$/)
      await expect(Number.parseFloat(from!.slice('translateY('.length))).toBeGreaterThan(0)
      await expect(reversal).toEqual({
        ...up('mail.check', 'mail.send'),
        leaving: { heading: 'mail.check', transforms: [from, to] },
      })
      await expect(to).toBe('translateY(100%)')
      for (const animation of first) await expect(animation.playState).toBe('idle')
      // One record leaves at a time, and it goes when its slide ends.
      const leaving = leavingCopy()
      await expect(leaving.children).toHaveLength(1)
      for (const animation of second) animation.play()
      await waitFor(() => expect(leaving.children).toHaveLength(0))
    } finally {
      recorder.stop()
    }
  },
}

/** The scrolling viewports inside `root`, not counting any inside a hidden copy. */
const viewports = (root: Element) =>
  [...root.querySelectorAll<HTMLElement>('*')].filter(
    (element) =>
      /auto|scroll/.test(getComputedStyle(element).overflowY) &&
      // A payload scrolls inside its own card; it is no record's viewport.
      !element.matches('[aria-label="Payload content"]'),
  )

/**
 * The old record leaves from where you scrolled it, the new one opens at its top, and
 * the divider holds still while the source, outside the slide, follows the new location.
 */
export const KeepsTheSourceStill: Story = {
  play: async (context) => {
    if (!(await inTestBrowser())) return
    const { rail, selected, leavingCopy, recorder, shows, select } = await surface(context)
    try {
      await shows('mail.send')
      const drawer = within(rail())
      const separator = drawer.getByRole('separator', { name: 'Resize detail and source panes' })
      const source = drawer.getByRole('region', { name: 'Accepted source' })
      const [code] = viewports(source)
      // Scroll the source past the next record's line, so following it must move the source.
      code!.scrollTop = 200
      await waitFor(() => expect(code!.scrollTop).toBe(200))
      const [detail] = viewports(selected())
      detail!.scrollTop = 300
      await waitFor(() => expect(detail!.scrollTop).toBe(300))
      const divider = separator.getBoundingClientRect().top

      await select('mail.check')
      // Hold the slide at its start, while both records show.
      await waitFor(() => expect(recorder.started).toHaveLength(2))
      for (const { animation } of recorder.started) animation.pause()
      const slide = await shows('mail.check')
      await expect(slide).toEqual(down('mail.send', 'mail.check'))
      const leaving = leavingCopy()
      await expect(viewports(leaving).map((element) => element.scrollTop)).toEqual([300])
      const live = viewports(selected()).filter((element) => !leaving.contains(element))
      await expect(live.map((element) => element.scrollTop)).toEqual([0])
      // The pane clips both records above the divider, which does not move.
      await expect(getComputedStyle(selected()).overflow).toBe('hidden')
      await expect(selected().getBoundingClientRect().bottom).toBeLessThanOrEqual(divider)
      await expect(separator.getBoundingClientRect().top).toBe(divider)
      // The source keeps its own viewport and scrolls it to the new record's line.
      await expect(viewports(source)).toEqual([code])
      await waitFor(() => {
        const line = code!.querySelector('[data-highlighted]')
        expect(line).toHaveAttribute('data-line', '4')
        const shown = code!.getBoundingClientRect()
        const { top, bottom } = line!.getBoundingClientRect()
        expect(top).toBeGreaterThanOrEqual(shown.top)
        expect(bottom).toBeLessThanOrEqual(shown.bottom)
      })
      await expect(code!.scrollTop).toBeLessThan(200)
    } finally {
      recorder.stop()
    }
  },
}

/**
 * Logs and emissions take their place in the Run's order too: ↑, ↓ and clicks slide
 * between them and tools.
 */
export const SlidesThroughLogsAndEmissions: Story = {
  play: async (context) => {
    if (!(await inTestBrowser())) return
    const { rail, recorder, shows, select } = await surface(context)
    try {
      await shows('mail.send')
      rail().focus()
      await press('{ArrowDown}')
      await expect(await shows('mail.check')).toEqual(down('mail.send', 'mail.check'))
      await press('{ArrowDown}')
      await expect(await shows('console.info #3')).toEqual(down('mail.check', 'console.info #3'))
      await press('{ArrowDown}')
      await expect(await shows('mail.archive')).toEqual(down('console.info #3', 'mail.archive'))
      await press('{ArrowUp}')
      await expect(await shows('console.info #3')).toEqual(up('mail.archive', 'console.info #3'))
      await select('emission #6')
      await expect(await shows('coral.emit #6')).toEqual(down('console.info #3', 'coral.emit #6'))
      await select('mail.notify')
      await expect(await shows('mail.notify')).toEqual(up('coral.emit #6', 'mail.notify'))
      rail().focus()
      await press('{ArrowDown}')
      await expect(await shows('coral.emit #6')).toEqual(down('mail.notify', 'coral.emit #6'))
      await press('{ArrowUp}')
      await expect(await shows('mail.notify')).toEqual(up('coral.emit #6', 'mail.notify'))
    } finally {
      recorder.stop()
    }
  },
}

/**
 * Lifecycle records, such as a Tool call ending or a request to cancel the Run,
 * slide in the Run's order like every other record inside it.
 */
export const SlidesThroughLifecycleEvents: Story = {
  play: async (context) => {
    if (!(await inTestBrowser())) return
    const { rail, recorder, shows, select } = await surface(context)
    const callEnded = 'Tool call ended: completed'
    const cancelRequested = 'Cancellation requested'
    try {
      await shows('mail.send')
      await select('emission #6')
      await shows('coral.emit #6')
      rail().focus()
      await press('{ArrowDown}')
      await expect(await shows(callEnded)).toEqual(down('coral.emit #6', callEnded))
      await press('{ArrowDown}')
      await expect(await shows(cancelRequested)).toEqual(down(callEnded, cancelRequested))
      await press('{ArrowUp}')
      await expect(await shows(callEnded)).toEqual(up(cancelRequested, callEnded))
      await press('{ArrowUp}')
      await expect(await shows('coral.emit #6')).toEqual(up(callEnded, 'coral.emit #6'))
      await select('cancel #8')
      await expect(await shows(cancelRequested)).toEqual(down('coral.emit #6', cancelRequested))
      await select('mail.send')
      await expect(await shows('mail.send')).toEqual(up(cancelRequested, 'mail.send'))
    } finally {
      recorder.stop()
    }
  },
}

/**
 * Every change but one between records of one Run, each at a known position, replaces
 * the record at once.
 */
export const ReplacesAtOnce: Story = {
  parameters: { initialEntry: `/workspaces/finance/activity?${expand}&record=${reminders.name}` },
  play: async (context) => {
    if (!(await inTestBrowser())) return
    const { recorder, shows, select, press: button } = await surface(context)
    try {
      await shows('Send reminders')
      // From the Run itself, which its Task orders, not its Run.
      await select('mail.send')
      await expect(await shows('mail.send')).toEqual(instant)
      // A tool of another Run.
      await select('audit.check')
      await expect(await shows('audit.check')).toEqual(instant)
      await select('mail.notify')
      await expect(await shows('mail.notify')).toEqual(instant)
      // A tool outside the loaded window has no known position.
      await button('Open unloaded tool')
      await expect(await shows('mail.unloaded')).toEqual(instant)
    } finally {
      recorder.stop()
    }
  },
}

/** With reduced motion, a move between tools replaces the record at once. */
export const ReducedMotion: Story = {
  play: async (context) => {
    if (!(await emulateReducedMotion(true))) return
    const { selected, recorder, shows, select } = await surface(context)
    try {
      await shows('mail.send')
      const [detail] = viewports(selected())
      detail!.scrollTop = 300
      await waitFor(() => expect(detail!.scrollTop).toBe(300))
      await select('mail.check')
      await expect(await shows('mail.check')).toEqual(instant)
      await expect(viewports(selected()).map((element) => element.scrollTop)).toEqual([0])
    } finally {
      recorder.stop()
      await emulateReducedMotion(false)
    }
  },
}

/** Without Web Animations, a move between tools replaces the record at once. */
export const WithoutWebAnimations: Story = {
  play: async (context) => {
    if (!(await inTestBrowser())) return
    const animate = Element.prototype.animate
    const { leavingCopy, recorder, shows, select } = await surface(context)
    try {
      await shows('mail.send')
      // @ts-expect-error A browser without Web Animations has no `animate`.
      delete Element.prototype.animate
      await select('mail.check')
      await expect(await shows('mail.check')).toEqual(instant)
      await expect(leavingCopy().children).toHaveLength(0)
    } finally {
      recorder.stop()
      Element.prototype.animate = animate
    }
  },
}
