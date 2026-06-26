import { chartBgColor, chartColor, scaleLinear } from '../utils'
import * as styles from './sparkline.css'

export interface SparklineProps {
  color?: string
  data: number[]
  height?: number
  width?: number
}

const PADDING = 2

export function Sparkline({ color, data, height = 24, width = 80 }: SparklineProps) {
  if (data.length < 2) return null

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
      className={styles.container}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      width={width}
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
