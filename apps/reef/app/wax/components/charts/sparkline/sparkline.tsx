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
   * `bars` draws one bar per point from a zero baseline, for counts per period.
   * A point at zero draws no bar.
   */
  variant?: 'line' | 'bars'
  /**
   * With `bars`, says what the bar at `index` shows: a hover lifts the bar under
   * the pointer and shows this in a tooltip. The chart is hidden from assistive
   * technology, so give its values in text nearby too.
   */
  describe?: (index: number) => ReactNode
  /** Stretches the chart across its container; `width` then sets only its proportions. */
  fill?: boolean
}

const PADDING = 2

export function Sparkline({
  color,
  data,
  height = 24,
  width = 80,
  variant = 'line',
  describe,
  fill = false,
}: SparklineProps) {
  if (data.length < 2) return null
  const className = fill ? styles.filled : styles.container
  if (variant === 'bars') {
    return (
      <Bars
        className={className}
        color={color}
        data={data}
        describe={describe}
        fill={fill}
        height={height}
        width={width}
      />
    )
  }

  const min = Math.min(...data)
  const max = Math.max(...data)
  const xScale = scaleLinear([0, data.length - 1], [PADDING, width - PADDING])
  const yScale = scaleLinear([min, max], [height - PADDING, PADDING])

  const points = data.map((v, i) => `${xScale(i)},${yScale(v)}`)
  const lineColor = chartColor(color)
  const bgColor = chartBgColor(color)

  const areaPoints = [
    ...points,
    `${width - PADDING},${height - PADDING}`,
    `${PADDING},${height - PADDING}`,
  ]

  return (
    <svg
      className={className}
      height={height}
      preserveAspectRatio={fill ? 'none' : undefined}
      viewBox={`0 0 ${width} ${height}`}
      width={fill ? '100%' : width}
    >
      <polygon fill={bgColor} opacity={0.25} points={areaPoints.join(' ')} />
      <polyline
        fill="none"
        points={points.join(' ')}
        stroke={lineColor}
        strokeLinejoin="round"
        strokeWidth={1.5}
      />
    </svg>
  )
}

/** A bar whose data end is rounded and whose baseline end is square. */
function barPath(x: number, y: number, w: number, h: number) {
  const r = Math.min(2, w / 4, h)
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`
}

function Bars({
  className,
  color,
  data,
  describe,
  fill,
  height,
  width,
}: Required<Pick<SparklineProps, 'height' | 'width' | 'fill'>> &
  Pick<SparklineProps, 'color' | 'data' | 'describe'> & { className: string }) {
  const [hovered, setHovered] = useState<number | null>(null)
  const max = Math.max(0, ...data)
  // Each bar owns its slot; a 2px surface gap separates neighbours.
  const slot = (width - 2 * PADDING) / data.length
  const barWidth = Math.max(1, slot - 2)
  // A bar of a nonzero point stays visible however far below the tallest it is.
  const barHeight = (value: number) =>
    value > 0 ? Math.max(2, (value / max) * (height - 2 * PADDING)) : 0
  const barColor = chartColor(color)

  // The bar under the pointer is its slot's, so a reader aims at a day, not a 2px mark.
  const track = (event: PointerEvent<SVGSVGElement>) => {
    const box = event.currentTarget.getBoundingClientRect()
    const x = ((event.clientX - box.left) / box.width) * width
    setHovered(Math.min(data.length - 1, Math.max(0, Math.floor((x - PADDING) / slot))))
  }

  const chart = (
    <svg
      aria-hidden
      className={className}
      height={height}
      preserveAspectRatio={fill ? 'none' : undefined}
      viewBox={`0 0 ${width} ${height}`}
      width={fill ? '100%' : width}
      onPointerMove={describe && track}
      onPointerLeave={describe && (() => setHovered(null))}
    >
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
            fill={barColor}
          />
        ) : null
      })}
    </svg>
  )
  if (!describe) return chart
  return (
    <Tooltip delay={0} content={describe(hovered ?? data.length - 1)}>
      <span className={className}>{chart}</span>
    </Tooltip>
  )
}
