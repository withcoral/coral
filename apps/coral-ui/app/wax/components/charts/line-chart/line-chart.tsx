import { chartBgColor, chartColor, computeNiceTicks, scaleLinear } from '../utils'
import * as styles from './line-chart.css'

export interface LineChartProps {
  height?: number
  labels?: string[]
  series: LineChartSeries[]
  width?: number
}

export interface LineChartSeries {
  color?: string
  data: number[]
  label: string
}

const PADDING = { bottom: 28, left: 44, right: 8, top: 8 }

export function LineChart({ height = 200, labels, series, width = 400 }: LineChartProps) {
  if (series.length === 0) return null

  const allValues = series.flatMap((s) => s.data)
  if (allValues.length === 0) return null

  const minValue = Math.min(...allValues)
  const maxValue = Math.max(...allValues)
  const ticks = computeNiceTicks(Math.min(0, minValue), maxValue, 4)
  const tickMin = ticks[0]
  const tickMax = ticks[ticks.length - 1]

  const maxPoints = Math.max(...series.map((s) => s.data.length))
  const plotWidth = width - PADDING.left - PADDING.right
  const plotHeight = height - PADDING.top - PADDING.bottom

  const yScale = scaleLinear([tickMin, tickMax], [plotHeight, 0])
  const xScale = scaleLinear([0, Math.max(maxPoints - 1, 1)], [0, plotWidth])

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

      {/* X-axis labels */}
      {labels?.map((label, i) => {
        const x = PADDING.left + xScale(i)
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
        const points = s.data.map(
          (v, i) => `${PADDING.left + xScale(i)},${PADDING.top + yScale(v)}`,
        )
        const color = chartColor(s.color)
        const bgColor = chartBgColor(s.color)

        // Area fill
        const firstX = PADDING.left + xScale(0)
        const lastX = PADDING.left + xScale(s.data.length - 1)
        const baseline = PADDING.top + yScale(tickMin)
        const areaPoints = [...points, `${lastX},${baseline}`, `${firstX},${baseline}`]

        return (
          <g key={si}>
            <polygon fill={bgColor} opacity={0.3} points={areaPoints.join(' ')} />
            <polyline
              fill="none"
              points={points.join(' ')}
              stroke={color}
              strokeLinejoin="round"
              strokeWidth={2}
            />
            {s.data.map((v, i) => (
              <circle
                cx={PADDING.left + xScale(i)}
                cy={PADDING.top + yScale(v)}
                fill={color}
                key={i}
                r={3}
              />
            ))}
          </g>
        )
      })}
    </svg>
  )
}
