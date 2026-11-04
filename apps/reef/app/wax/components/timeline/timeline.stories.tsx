import type { Meta, StoryObj } from '@storybook/react-vite'
import { useRef, useState } from 'react'
import { expect, fireEvent, fn, userEvent, waitFor, within } from 'storybook/test'

import { timeTicks, type TimeRange } from './range'
import {
  TimelineAxis,
  TimelineBar,
  TimelineGrid,
  TimelineMark,
  TimelineMinimap,
  useTimelineWheel,
} from './timeline'

const bounds = { from: 0, to: 400_000 }
const seconds = (instant: number) => {
  const total = Math.round(instant / 1000)
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, '0')}`
}

const row = { position: 'relative', height: 26 } as const

/** Every way a bar can end, with and without a wait, on one axis. */
function Bars() {
  const ticks = timeTicks(bounds, 0, 8)
  return (
    <div style={{ width: 720, position: 'relative' }}>
      <div style={{ height: 24 }}>
        <TimelineAxis range={bounds} ticks={ticks} label={seconds} />
      </div>
      <div style={{ position: 'relative' }}>
        <TimelineGrid range={bounds} ticks={ticks} />
        <div style={row}>
          <TimelineBar
            range={bounds}
            now={400_000}
            start={0}
            end={{ kind: 'ended', at: 120_000 }}
            tone="blue"
          >
            2m 00s
          </TimelineBar>
        </div>
        <div style={row}>
          <TimelineBar
            range={bounds}
            now={400_000}
            start={40_000}
            end={{ kind: 'ended', at: 230_000 }}
            waitUntil={220_000}
            tone="amber"
          >
            waited 3m 00s for approval · ran 10.0s
          </TimelineBar>
        </div>
        <div style={row}>
          <TimelineBar
            range={bounds}
            now={400_000}
            start={250_000}
            end={{ kind: 'ongoing' }}
            tone="blue"
          >
            2m 30s so far
          </TimelineBar>
        </div>
        <div style={row}>
          <TimelineBar
            range={bounds}
            now={400_000}
            start={180_000}
            end={{ kind: 'unknown' }}
            tone="purple"
          >
            end unknown
          </TimelineBar>
        </div>
        <div style={row}>
          <TimelineBar
            range={bounds}
            now={400_000}
            start={300_000}
            end={{ kind: 'ended', at: 300_040 }}
            tone="red"
          >
            40ms
          </TimelineBar>
        </div>
      </div>
    </div>
  )
}

const meta = {
  title: 'Wax/Timeline',
  component: Bars,
  parameters: { layout: 'centered' },
} satisfies Meta<typeof Bars>
export default meta
type Story = StoryObj<typeof meta>

export const BarEnds: Story = {}

/** A log is a diamond, an emission a circle and a lifecycle Event a square: each is a button
 * named for what happened. */
export const Marks: Story = {
  args: { onSelect: fn() } as never,
  render: (args) => {
    const { onSelect } = args as unknown as { onSelect: (name: string) => void }
    return (
      <div style={{ width: 480, ...row }}>
        <TimelineBar
          range={bounds}
          now={400_000}
          start={0}
          end={{ kind: 'ended', at: 400_000 }}
          tone="blue"
        />
        <TimelineMark
          range={bounds}
          at={100_000}
          shape="diamond"
          label="log #3"
          onSelect={() => onSelect('log #3')}
        />
        <TimelineMark
          range={bounds}
          at={250_000}
          shape="circle"
          label="emission #4"
          onSelect={() => onSelect('emission #4')}
        />
        <TimelineMark
          range={bounds}
          at={390_000}
          shape="square"
          label="execution ended"
          onSelect={() => onSelect('execution ended')}
        />
      </div>
    )
  },
  play: async ({ canvasElement, args }) => {
    await userEvent.click(within(canvasElement).getByRole('button', { name: 'emission #4' }))
    await expect(
      (args as unknown as { onSelect: ReturnType<typeof fn> }).onSelect,
    ).toHaveBeenCalledWith('emission #4')
  },
}

function MinimapHarness() {
  const [range, setRange] = useState<TimeRange>({ from: 100_000, to: 200_000 })
  return (
    <div style={{ width: 600 }}>
      <TimelineMinimap
        bounds={bounds}
        range={range}
        onRange={setRange}
        lanes={[
          { start: 0, end: 400_000, tone: 'neutral' },
          { start: 20_000, end: 60_000, tone: 'blue' },
          { start: 48_000, end: 252_000, tone: 'blue' },
          { start: 49_000, end: 235_000, tone: 'amber' },
        ]}
        ticks={timeTicks(bounds, 0, 8)}
        tickLabel={seconds}
        label={`${seconds(range.from)} to ${seconds(range.to)}`}
      />
    </div>
  )
}

/** ← and → move the frame by a tenth; + zooms in around its middle; double-click frames all. */
export const Minimap: Story = {
  render: () => <MinimapHarness />,
  play: async ({ canvasElement }) => {
    const slider = within(canvasElement).getByRole('slider', { name: 'Visible time' })
    await expect(slider).toHaveAttribute('aria-valuetext', '1:40 to 3:20')
    slider.focus()
    await userEvent.keyboard('{ArrowRight}')
    await expect(slider).toHaveAttribute('aria-valuetext', '1:50 to 3:30')
    await userEvent.keyboard('+')
    await expect(slider).toHaveAttribute('aria-valuetext', '2:15 to 3:05')
    await userEvent.dblClick(slider)
    await expect(slider).toHaveAttribute('aria-valuetext', '0:00 to 6:40')
  },
}

function WheelHarness() {
  const [range, setRange] = useState<TimeRange>(bounds)
  const area = useRef<HTMLDivElement>(null)
  const track = useRef<HTMLDivElement>(null)
  useTimelineWheel(area, track, bounds, setRange)
  return (
    <div ref={area} style={{ width: 400, padding: 20 }}>
      <div ref={track} data-testid="track" style={{ height: 24, position: 'relative' }}>
        <TimelineAxis range={range} ticks={timeTicks(range, 0, 4)} label={seconds} />
      </div>
      <output aria-label="Range">{`${seconds(range.from)} to ${seconds(range.to)}`}</output>
    </div>
  )
}

/** Cmd/Ctrl + wheel zooms around the pointer; Shift + wheel pans. */
export const Wheel: Story = {
  render: () => <WheelHarness />,
  play: async ({ canvasElement }) => {
    const track = within(canvasElement).getByTestId('track')
    const output = within(canvasElement).getByRole('status', { name: 'Range' })
    const rect = track.getBoundingClientRect()
    // Zoom in at the track's start: the start stays where it is.
    fireEvent.wheel(track, { ctrlKey: true, deltaY: -231, clientX: rect.left })
    await waitFor(() => expect(output).toHaveTextContent(/^0:00 to 3:20$/))
    fireEvent.wheel(track, { shiftKey: true, deltaY: rect.width / 2 })
    await waitFor(() => expect(output).toHaveTextContent(/^1:40 to 5:00$/))
  },
}
