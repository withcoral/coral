import type { Meta, StoryObj } from '@storybook/react-vite'
import { useMemo } from 'react'
import { expect, userEvent, waitFor, within } from 'storybook/test'

import { press } from '../../../../.storybook/browser'
import * as Timeline from './index'

interface Span {
  label: string
  from: number
  to: number | null
  color?: Timeline.BarColor
  end?: Timeline.BarEnd
  wait?: number
  marks?: { at: number; shape: Timeline.MarkShape; label: string }[]
}

const seconds = (time: number) => `${(time / 1000).toFixed(2)}s`

/** A few intervals on one axis, with the controls a caller puts around them. */
function Demo({ spans, end, folded }: { spans: Span[]; end: number; folded: boolean }) {
  const busy = useMemo(
    () => spans.map((span) => ({ from: span.from, to: span.to ?? end })),
    [spans, end],
  )
  const scale = useMemo(() => {
    const range = { from: 0, to: end }
    return folded ? Timeline.foldedScale(range, busy) : Timeline.wallScale(range)
  }, [folded, busy, end])
  const controller = Timeline.useTimeline(scale)
  const { view } = Timeline.useTimelineView(controller)
  const shown = `${seconds(scale.toTime(view.from))} – ${seconds(scale.toTime(view.to))}`
  return (
    <Timeline.Root controller={controller} style={{ display: 'grid', gap: 8, padding: 24 }}>
      <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
        <button type="button" onClick={() => controller.zoom(1 / 2)}>
          Zoom in
        </button>
        <button type="button" onClick={() => controller.zoom(2)}>
          Zoom out
        </button>
        <button type="button" onClick={() => controller.fit()}>
          Fit
        </button>
        <button type="button" onClick={() => controller.focus({ from: 2_000, to: 3_000 })}>
          Show 2 s to 3 s
        </button>
        <output aria-label="Shown range">{shown}</output>
      </div>
      <Timeline.Minimap
        controller={controller}
        label="Visible time range"
        valueText={shown}
        description={`${spans.length} intervals.`}
      >
        {spans.map((span, index) => (
          <Timeline.MinimapSpan
            key={span.label}
            from={scale.toAxis(span.from)}
            to={span.to === null ? scale.extent : scale.toAxis(span.to)}
            lane={index}
            lanes={spans.length}
            color={span.color}
            end={span.end}
            flagged={span.color === 'red'}
          />
        ))}
      </Timeline.Minimap>
      <div style={{ height: 24 }}>
        <Timeline.Track>
          <Timeline.Axis controller={controller} format={seconds} />
          <Timeline.Folds
            controller={controller}
            label={(fold) => `${seconds(fold.to - fold.from)} compressed`}
          />
        </Timeline.Track>
      </div>
      {spans.map((span) => (
        <div key={span.label} style={{ height: 24 }} aria-label={span.label} role="group">
          <Timeline.Track>
            <Timeline.Bar
              from={scale.toAxis(span.from)}
              to={span.to === null ? scale.extent : scale.toAxis(span.to)}
              color={span.color}
              end={span.end}
              title={span.label}
            />
            {span.wait !== undefined && (
              <Timeline.Bar wait from={scale.toAxis(span.from)} to={scale.toAxis(span.wait)} />
            )}
            {span.marks?.map((mark) => (
              <Timeline.Mark
                key={mark.label}
                at={scale.toAxis(mark.at)}
                shape={mark.shape}
                label={mark.label}
              />
            ))}
          </Timeline.Track>
        </div>
      ))}
    </Timeline.Root>
  )
}

const meta = {
  title: 'Wax/Timeline',
  component: Demo,
  parameters: { layout: 'fullscreen' },
} satisfies Meta<typeof Demo>
export default meta
type Story = StoryObj<typeof meta>

const spans: Span[] = [
  { label: 'Whole', from: 0, to: 10_000, color: 'purple' },
  { label: 'First call', from: 500, to: 2_500, wait: 1_200 },
  {
    label: 'Second call',
    from: 3_000,
    to: 9_000,
    marks: [
      { at: 4_000, shape: 'circle', label: 'log' },
      { at: 6_000, shape: 'diamond', label: 'emission' },
      { at: 8_000, shape: 'triangle', label: 'cancellation' },
    ],
  },
  // A hundredth of a millisecond: it still shows.
  { label: 'Instant', from: 5_000, to: 5_000.01, color: 'green' },
]

const range = (canvasElement: HTMLElement) =>
  within(canvasElement).getByRole('status', { name: 'Shown range' })
/** Where a bar starts and ends, as shares of its track's width. */
function barShares(group: HTMLElement) {
  const track = group.querySelector('[data-timeline-track]')!.getBoundingClientRect()
  const bar = group.querySelector('[data-timeline-bar]')!.getBoundingClientRect()
  return [(bar.left - track.left) / track.width, (bar.right - track.left) / track.width]
}

/** Bars sit where their times are, and every way to move the view keeps the label true. */
export const ZoomAndPan: Story = {
  args: { spans, end: 10_000, folded: false },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const first = canvas.getByRole('group', { name: 'First call' })
    const [from, to] = barShares(first)
    expect(from).toBeCloseTo(0.05, 2)
    expect(to).toBeCloseTo(0.25, 2)
    // An interval far shorter than a pixel keeps a visible width.
    const instant = canvas
      .getByRole('group', { name: 'Instant' })
      .querySelector('[data-timeline-bar]')!
    expect(instant.getBoundingClientRect().width).toBeGreaterThanOrEqual(3)
    await expect(range(canvasElement)).toHaveTextContent('0.00s – 10.00s')

    await userEvent.click(canvas.getByRole('button', { name: 'Show 2 s to 3 s' }))
    await expect(range(canvasElement)).toHaveTextContent('1.92s – 3.08s')
    // The first call ends at 2.5 s: half way across the 2 s to 3 s the view shows.
    await waitFor(() => expect(barShares(first)[1]).toBeCloseTo(0.5, 1))

    // The window of the minimap takes the keys: arrows pan, + and − zoom, Home fits.
    const window = canvas.getByRole('slider', { name: 'Visible time range' })
    window.focus()
    await press('{ArrowRight}')
    await expect(range(canvasElement)).toHaveTextContent('2.04s – 3.20s')
    expect(window).toHaveAttribute('aria-valuetext', '2.04s – 3.20s')
    await press('-')
    await expect(range(canvasElement)).toHaveTextContent('1.75s – 3.49s')
    await press('{Home}')
    await expect(range(canvasElement)).toHaveTextContent('0.00s – 10.00s')

    // A wheel with Ctrl zooms around the pointer, which stays over the same time.
    const track = first.querySelector('[data-timeline-track]')!
    const box = track.getBoundingClientRect()
    track.dispatchEvent(
      new WheelEvent('wheel', {
        bubbles: true,
        cancelable: true,
        ctrlKey: true,
        deltaY: -100,
        clientX: box.left + box.width * 0.25,
        clientY: box.top + 4,
      }),
    )
    // 2.5 s was a quarter of the way along, and still is.
    await waitFor(() => expect(barShares(first)[1]).toBeCloseTo(0.25, 2))
    // A wheel with Shift pans.
    const before = range(canvasElement).textContent
    track.dispatchEvent(
      new WheelEvent('wheel', { bubbles: true, cancelable: true, shiftKey: true, deltaY: 200 }),
    )
    await waitFor(() => expect(range(canvasElement).textContent).not.toBe(before))
  },
}

/** A long gap folds to a band that says what it hides. The label still reads true time. */
export const Folded: Story = {
  args: {
    end: 600_000,
    folded: true,
    spans: [
      { label: 'Early', from: 0, to: 4_000 },
      { label: 'Late', from: 596_000, to: 600_000, color: 'purple' },
    ],
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const fold = await waitFor(() => {
      const element = canvasElement.querySelector('[data-timeline-fold]')
      expect(element).not.toBeNull()
      return element!
    })
    expect(fold).toHaveAttribute('title', '592.00s compressed')
    await expect(range(canvasElement)).toHaveTextContent('0.00s – 600.00s')
    // Both bars keep their length: each is 4 s of the 8 s the axis spends on work.
    const early = barShares(canvas.getByRole('group', { name: 'Early' }))
    const late = barShares(canvas.getByRole('group', { name: 'Late' }))
    expect(early[1] - early[0]).toBeCloseTo(late[1] - late[0], 2)
    expect(early[1] - early[0]).toBeGreaterThan(0.4)
  },
}

/** How an interval ends reads from its shape: closed, open while work goes on, or unknown. */
export const Ends: Story = {
  args: {
    end: 10_000,
    folded: false,
    spans: [
      { label: 'Ended', from: 0, to: 4_000 },
      { label: 'Ongoing', from: 2_000, to: null, end: 'ongoing' },
      { label: 'Unknown', from: 6_000, to: 6_000, end: 'unknown', color: 'amber' },
      { label: 'Failed', from: 1_000, to: 3_000, color: 'red' },
    ],
  },
  play: async ({ canvasElement }) => {
    const kinds = [...canvasElement.querySelectorAll('[data-timeline-bar]')].map((bar) =>
      bar.getAttribute('data-timeline-bar'),
    )
    // The tracks draw each end's own shape.
    expect(kinds.slice(-4)).toEqual(['ended', 'ongoing', 'unknown', 'ended'])
    // The overview draws the same ends by shape, and a tick across the line that needs a
    // look, so neither reads from colour alone. Its lines are shapes with no role.
    const lines = [...canvasElement.querySelectorAll<HTMLElement>('[data-timeline-span]')]
    expect(lines.map((line) => line.dataset.timelineSpanEnd)).toEqual([
      'ended',
      'ongoing',
      'unknown',
      'ended',
    ])
    const [ended, ongoing, unknown, failed] = lines.map((line) => getComputedStyle(line))
    expect(ended!.maskImage).toBe('none')
    expect(ongoing!.maskImage).toContain('linear-gradient')
    expect(unknown!.maskImage).toContain('repeating-linear-gradient')
    expect(getComputedStyle(lines[3]!, '::before').height).toBe('8px')
    expect(getComputedStyle(lines[0]!, '::before').content).toBe('none')
    expect(failed!.minWidth).toBe('4px')
    // What the lines cannot say one by one, the window says in words.
    await expect(
      within(canvasElement).getByRole('slider', { name: 'Visible time range' }),
    ).toHaveAccessibleDescription('4 intervals.')
  },
}
