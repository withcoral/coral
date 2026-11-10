import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const container = style({
  display: 'inline-block',
  verticalAlign: 'middle',
})
export const filled = style({ display: 'block', width: '100%' })
// The axis bars grow from: a recessive hairline, so a series of zeros still shows.
export const baseline = style({ stroke: theme.surface.onMainContent, strokeWidth: 1 })
// The bar under the pointer lifts, so the reader sees which point the tooltip names.
export const barHovered = style({ fill: theme.content.secondary })
