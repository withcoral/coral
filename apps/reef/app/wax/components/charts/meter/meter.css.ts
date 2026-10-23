import { style, styleVariants } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

const track = style({
  display: 'flex',
  borderRadius: 4,
  overflow: 'hidden',
  background: theme.surface.onMainContent,
})
export const size = styleVariants({
  medium: [track, { height: 8 }],
  small: [track, { height: 4 }],
})
// One quiet tone for every meter: the text beside it says what it counts.
export const fill = style({ minWidth: 2, background: theme.chart.neutral.fill })
