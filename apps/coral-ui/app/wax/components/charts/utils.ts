import { theme } from '@/wax/theme/theme.css'

export function computeNiceTicks(min: number, max: number, count: number): number[] {
  if (max === min) return [min]
  const range = max - min
  const roughStep = range / Math.max(count - 1, 1)
  const magnitude = Math.pow(10, Math.floor(Math.log10(roughStep)))
  const residual = roughStep / magnitude

  let niceStep: number
  if (residual <= 1.5) niceStep = magnitude
  else if (residual <= 3) niceStep = 2 * magnitude
  else if (residual <= 7) niceStep = 5 * magnitude
  else niceStep = 10 * magnitude

  const niceMin = Math.floor(min / niceStep) * niceStep
  const niceMax = Math.ceil(max / niceStep) * niceStep

  const ticks: number[] = []
  for (let v = niceMin; v <= niceMax + niceStep * 0.5; v += niceStep) {
    ticks.push(Math.round(v * 1e10) / 1e10)
  }
  return ticks
}

export function scaleLinear(domain: [number, number], range: [number, number]) {
  const [d0, d1] = domain
  const [r0, r1] = range
  const dSpan = d1 - d0 || 1
  return (value: number) => r0 + ((value - d0) / dSpan) * (r1 - r0)
}

// Solid, saturated fill colours for chart elements (bars, lines, dots).
// Uses causeGraph accent tokens (palette step 10) for vibrant fills,
// with gray falling back to pill color since causeGraph has no gray.
const chartFillMap: Record<string, string> = {
  amber: theme.causeGraph.amber.accent,
  blue: theme.causeGraph.blue.accent,
  gray: theme.pill.gray.color,
  green: theme.causeGraph.green.accent,
  orange: theme.causeGraph.orange.accent,
  purple: theme.causeGraph.purple.accent,
  red: theme.causeGraph.red.accent,
}

// Soft tinted background for area fills beneath lines.
const chartBgMap: Record<string, string> = {
  amber: theme.pill.amber.background,
  blue: theme.pill.blue.background,
  gray: theme.pill.gray.background,
  green: theme.pill.green.background,
  orange: theme.pill.orange.background,
  purple: theme.pill.purple.background,
  red: theme.pill.red.background,
}

export function chartBgColor(name?: string): string {
  return chartBgMap[name ?? 'blue'] ?? chartBgMap.blue
}

export function chartColor(name?: string): string {
  return chartFillMap[name ?? 'blue'] ?? chartFillMap.blue
}
