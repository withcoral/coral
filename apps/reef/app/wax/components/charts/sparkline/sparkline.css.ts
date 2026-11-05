import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const container = style({
  display: 'inline-block',
  verticalAlign: 'middle',
})
export const filled = style({ display: 'block', width: '100%' })
// A recessive hairline that finds the point under the pointer.
export const crosshair = style({ stroke: theme.content.tertiary, strokeWidth: 1 })
// A ring in the surface colour keeps the marker legible where it crosses the line.
export const marker = style({ stroke: theme.surface.mainContent, strokeWidth: 2 })
// The axis bars grow from: a recessive hairline, so a series of zeros still shows.
export const baseline = style({ stroke: theme.surface.onMainContent, strokeWidth: 1 })
// The bar under the pointer lifts, so the reader sees which point the tooltip names.
export const barHovered = style({ fill: theme.content.secondary })
