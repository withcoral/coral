import { chartColor, computeNiceTicks, scaleLinear } from '../utils'
import * as styles from './bar-chart.css'

export interface BarChartProps {
  data: { color?: string; label: string; value: number }[]
  height?: number
  width?: number
}

const PADDING = { bottom: 28, left: 44, right: 8, top: 8 }
const BAR_RADIUS = 3

export function BarChart({ data, height = 200, width = 400 }: BarChartProps) {
  if (data.length === 0) return null

  const maxValue = Math.max(...data.map((d) => d.value), 0)
  const ticks = computeNiceTicks(0, maxValue, 4)
  const tickMax = ticks[ticks.length - 1]

  const plotWidth = width - PADDING.left - PADDING.right
  const plotHeight = height - PADDING.top - PADDING.bottom

  const yScale = scaleLinear([0, tickMax], [plotHeight, 0])
  const barWidth = Math.max(1, (plotWidth / data.length) * 0.7)
  const barGap = (plotWidth / data.length) * 0.3

  return (
    <svg
      className={styles.container}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      width={width}
    >
      {/* Gridlines */}
      {ticks.map((tick) => {
        const y = PADDING.top + yScale(tick)
        return (
          <g key={tick}>
            <line
              className={styles.gridLine}
              x1={PADDING.left}
              x2={width - PADDING.right}
              y1={y}
              y2={y}
            />
            <text
              className={styles.axisText}
              dominantBaseline="middle"
              textAnchor="end"
              x={PADDING.left - 6}
              y={y}
            >
              {tick}
            </text>
          </g>
        )
      })}

      {/* Bars */}
      {data.map((d, i) => {
        const barHeight = Math.max(0, plotHeight - yScale(d.value))
        const x = PADDING.left + i * (barWidth + barGap) + barGap / 2
        const y = PADDING.top + yScale(d.value)
        const labelX = x + barWidth / 2
        const labelY = height - PADDING.bottom + 14

        return (
          <g key={i}>
            <rect
              fill={chartColor(d.color)}
              height={barHeight}
              rx={BAR_RADIUS}
              ry={BAR_RADIUS}
              width={barWidth}
              x={x}
              y={y}
            />
            <text
              className={styles.axisText}
              dominantBaseline="middle"
              textAnchor="middle"
              x={labelX}
              y={labelY}
            >
              {d.label.length > 8 ? d.label.slice(0, 7) + '\u2026' : d.label}
            </text>
          </g>
        )
      })}
    </svg>
  )
}
