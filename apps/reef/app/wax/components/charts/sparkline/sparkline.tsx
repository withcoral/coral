import { useState, type PointerEvent, type ReactNode } from 'react'

import { Tooltip } from '@/wax/components/tooltip'

import { chartBgColor, chartColor, scaleLinear } from '../utils'
import * as styles from './sparkline.css'

export interface SparklineProps {
  color?: string
  data: readonly number[]
  height?: number
  width?: number
  /**
   * The value the top edge stands for, so several lines share one scale.
   * Defaults to the data's largest value. The bottom edge is always zero.
   */
  max?: number
  /**
   * Says what the point at `index` shows. With it, a hover snaps a crosshair
   * to the nearest point and shows this in a tooltip. The line itself is
   * hidden from assistive technology, so give its values in text nearby too.
   */
  describe?: (index: number) => ReactNode
  /**
   * `bars` draws one bar per point over a baseline, for counts per period. A
   * point at zero draws no bar, and the bar under the pointer lifts.
   */
  variant?: 'line' | 'bars'
  /** Stretches the chart across its container; `width` then sets only the drawing's proportions. */
  fill?: boolean
}

/** A bar whose data end is rounded and whose baseline end is square. */
function barPath(x: number, y: number, w: number, h: number) {
  const r = Math.min(2, w / 4, h)
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`
}

const PADDING = 2

export function Sparkline({
  color,
  data,
  height = 24,
  width = 80,
  max = Math.max(0, ...data),
  describe,
  variant = 'line',
  fill = false,
}: SparklineProps) {
  const [hovered, setHovered] = useState<number | null>(null)
  if (data.length < 2) return null

  const xScale = scaleLinear([0, data.length - 1], [PADDING, width - PADDING])
  const yScale = scaleLinear([0, max], [height - PADDING, PADDING])

  const points = data.map((v, i) => `${xScale(i)},${yScale(Math.min(v, max))}`)
  const lineColor = chartColor(color)
  const bgColor = chartBgColor(color)

  const areaPoints = [
    ...points,
    `${width - PADDING},${height - PADDING}`,
    `${PADDING},${height - PADDING}`,
  ]

  // Each bar owns its slot; a 2px surface gap separates neighbours.
  const slot = (width - 2 * PADDING) / data.length
  const barWidth = Math.max(1, slot - 2)
  // A bar of a nonzero point stays visible however far below `max` it is.
  const barHeight = (value: number) =>
    value > 0 ? Math.max(2, (Math.min(value, max) / (max || 1)) * (height - 2 * PADDING)) : 0

  // The point under the pointer: its slot for bars, the nearest point for a line,
  // so a reader aims at a day, not at a 2px mark.
  const track = (event: PointerEvent<SVGSVGElement>) => {
    const box = event.currentTarget.getBoundingClientRect()
    const x = ((event.clientX - box.left) / box.width) * width
    const index =
      variant === 'bars'
        ? Math.floor((x - PADDING) / slot)
        : Math.round((x - PADDING) / ((width - 2 * PADDING) / (data.length - 1)))
    setHovered(Math.min(data.length - 1, Math.max(0, index)))
  }

  const chart = (
    <svg
      aria-hidden
      className={fill ? styles.filled : styles.container}
      height={height}
      preserveAspectRatio={fill ? 'none' : undefined}
      viewBox={`0 0 ${width} ${height}`}
      width={fill ? '100%' : width}
      onPointerMove={describe && track}
      onPointerLeave={describe && (() => setHovered(null))}
    >
      {variant === 'bars' ? (
        <>
          <line
            className={styles.baseline}
            x1={PADDING}
            x2={width - PADDING}
            y1={height - PADDING + 0.5}
            y2={height - PADDING + 0.5}
          />
          {data.map((value, i) => {
            const h = barHeight(value)
            return h > 0 ? (
              <path
                key={i}
                className={i === hovered ? styles.barHovered : undefined}
                d={barPath(
                  PADDING + i * slot + (slot - barWidth) / 2,
                  height - PADDING - h,
                  barWidth,
                  h,
                )}
                fill={lineColor}
              />
            ) : null
          })}
        </>
      ) : (
        <>
          <polygon fill={bgColor} opacity={0.25} points={areaPoints.join(' ')} />
          <polyline
            fill="none"
            points={points.join(' ')}
            stroke={lineColor}
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
          />
        </>
      )}
      {variant === 'line' && describe && hovered !== null && (
        <>
          <line
            className={styles.crosshair}
            x1={xScale(hovered)}
            x2={xScale(hovered)}
            y1={0}
            y2={height}
          />
          <circle
            className={styles.marker}
            cx={xScale(hovered)}
            cy={yScale(Math.min(data[hovered]!, max))}
            fill={lineColor}
            r={3}
          />
        </>
      )}
    </svg>
  )
  if (!describe) return chart
  return (
    <Tooltip delay={0} content={describe(hovered ?? data.length - 1)}>
      <span className={fill ? styles.filled : styles.container}>{chart}</span>
    </Tooltip>
  )
}
