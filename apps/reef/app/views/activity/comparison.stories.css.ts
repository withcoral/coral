import { globalStyle, style } from '@vanilla-extract/css'
import { tracePane } from './task-activity.css'

// The real Activity reader and root list stay mounted. In historical comparisons only,
// replace its painted waterfall with a snapshot via a portal into the same pane.
export const historical = style({ height: '100%', minHeight: 0 })
export const snapshot = style({
  display: 'flex',
  flexDirection: 'column',
  flex: '1 1 0',
  minHeight: 0,
  minWidth: 0,
})
globalStyle(`.${historical} .${tracePane} > [data-activity-waterfall]`, { display: 'none' })
