import { chartBgColor, chartColor, computeNiceTicks, scaleLinear } from '../utils'
import * as styles from './line-chart.css'

export interface LineChartProps {
  /** Names the chart as an image. Say what it shows, so the reader needs no picture. */
  ariaLabel?: string
  /** Marks each point with a dot. Turn it off for dense series. */
  dots?: boolean
  /** The text of a y-axis tick. */
  formatTick?: (value: number) => string
  height?: number
  /** One per point. An empty label leaves that point's tick blank. */
  labels?: string[]
  series: LineChartSeries[]
  width?: number
}

export interface LineChartSeries {
  color?: string
  /** A null point has no value: the line breaks there. */
  data: (number | null)[]
  label: string
}

const PADDING = { bottom: 28, right: 8, top: 8 }
// The y-axis labels end this far from the plot.
const TICK_GAP = 6

/** The width left of the plot: enough for the longest y-axis label, and at least 44px.
 * One character of the 11px axis text is about 6.6px wide. */
function gutter(tickLabels: string[]) {
  const longest = Math.max(...tickLabels.map((label) => label.length))
  return Math.max(44, Math.ceil(longest * 6.6) + TICK_GAP)
}

/** The runs of consecutive points that have a value, as [index, value] pairs. */
function segments(data: (number | null)[]) {
  const runs: [number, number][][] = []
  let run: [number, number][] = []
  data.forEach((value, index) => {
    if (value === null) {
      if (run.length) runs.push(run)
      run = []
    } else run.push([index, value])
  })
  if (run.length) runs.push(run)
  return runs
}

export function LineChart({
  ariaLabel,
  dots = true,
  formatTick = String,
  height = 200,
  labels,
  series,
  width = 400,
}: LineChartProps) {
  if (series.length === 0) return null

  const allValues = series.flatMap((s) => s.data).filter((v) => v !== null)
  if (allValues.length === 0) return null

  const minValue = Math.min(...allValues)
  const maxValue = Math.max(...allValues)
  const ticks = computeNiceTicks(Math.min(0, minValue), maxValue, 4)
  const tickMin = ticks[0]
  const tickMax = ticks[ticks.length - 1]

  const tickLabels = ticks.map(formatTick)
  const left = gutter(tickLabels)
  const maxPoints = Math.max(...series.map((s) => s.data.length))

  // Both scales map straight to SVG coordinates.
  const yScale = scaleLinear([tickMin, tickMax], [height - PADDING.bottom, PADDING.top])
  const xScale = scaleLinear([0, Math.max(maxPoints - 1, 1)], [left, width - PADDING.right])

  return (
    <svg
      className={styles.container}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      width={width}
      role={ariaLabel ? 'img' : undefined}
      aria-label={ariaLabel}
    >
      {/* Gridlines */}
      {ticks.map((tick, i) => {
        const y = yScale(tick)
        return (
          <g key={tick}>
            <line className={styles.gridLine} x1={left} x2={width - PADDING.right} y1={y} y2={y} />
            <text
              className={styles.axisText}
              dominantBaseline="middle"
              textAnchor="end"
              x={left - TICK_GAP}
              y={y}
            >
              {tickLabels[i]}
            </text>
          </g>
        )
      })}

      {/* X-axis labels */}
      {labels?.map((label, i) => {
        if (!label) return null
        const x = xScale(i)
        return (
          <text
            className={styles.axisText}
            dominantBaseline="middle"
            key={i}
            textAnchor="middle"
            x={x}
            y={height - PADDING.bottom + 14}
          >
            {label.length > 8 ? label.slice(0, 7) + '\u2026' : label}
          </text>
        )
      })}

      {/* Series */}
      {series.map((s, si) => {
        const color = chartColor(s.color)
        const bgColor = chartBgColor(s.color)
        const baseline = yScale(tickMin)

        return (
          <g key={si}>
            {segments(s.data).map((run) => {
              const points = run.map(([i, v]) => `${xScale(i)},${yScale(v)}`)
              // Area fill
              const firstX = xScale(run[0][0])
              const lastX = xScale(run[run.length - 1][0])
              const areaPoints = [...points, `${lastX},${baseline}`, `${firstX},${baseline}`]
              return (
                <g key={run[0][0]}>
                  <polygon fill={bgColor} opacity={0.3} points={areaPoints.join(' ')} />
                  <polyline
                    fill="none"
                    points={points.join(' ')}
                    stroke={color}
                    strokeLinejoin="round"
                    strokeWidth={2}
                  />
                  {/* A lone point has no line, so it always shows as a dot. */}
                  {(dots || run.length === 1) &&
                    run.map(([i, v]) => (
                      <circle cx={xScale(i)} cy={yScale(v)} fill={color} key={i} r={dots ? 3 : 2} />
                    ))}
                </g>
              )
            })}
          </g>
        )
      })}
    </svg>
  )
}
