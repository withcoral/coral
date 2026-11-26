// Story-only snapshot from tree 78baa17366f1bce6d57e64c702f82fd2a3030036.
// Historical comparison; not used by the production Activity page.
import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'
import { SINGLE_PANE_MAX_WIDTH } from './breakpoint'

export const narrow = `(max-width: ${SINGLE_PANE_MAX_WIDTH}px)`

export const pane = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  flex: '1 1 0',
  minWidth: 0,
  minHeight: 0,
})
export const heading = style({ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 })
export const title = style({ minWidth: 0, flex: '1 1 auto' })
/** Only the single-pane flow needs a way back to the list. */
export const back = style({ display: 'none', '@media': { [narrow]: { display: 'inline-flex' } } })
export const toolbar = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  columnGap: 12,
  rowGap: 8,
  minWidth: 0,
})
export const zoom = style({ display: 'inline-flex', alignItems: 'center', gap: 2 })
export const range = style({ marginInlineStart: 'auto', minWidth: 0, textAlign: 'end' })
/**
 * The time breakdown is as tall as what it says, so the controls sit right under it. Its
 * parts and notes keep their places while values load or a note goes, so the table under
 * it does not move. On a short screen it gives its room to the table.
 */
export const breakdown = style({
  borderBlockEnd: `1px solid ${theme.stroke.primary}`,
  flex: '0 1 auto',
  maxHeight: 'clamp(104px, 28vh, 196px)',
  minHeight: 0,
  overflowY: 'auto',
})
/** One line that is always there: what loads, or how to read the axis. */
export const status = style({
  display: 'flex',
  alignItems: 'center',
  gap: 4,
  flexShrink: 0,
  height: 22,
  minWidth: 0,
  overflow: 'hidden',
  whiteSpace: 'nowrap',
})
/** The table always has room for its heading and a few records. */
export const table = style({
  display: 'flex',
  flexDirection: 'column',
  flex: '1 1 0',
  minHeight: 132,
})
/** A timeline cell: the track fills the row, so bars centre in it. */
export const trackCell = style({ alignSelf: 'stretch', minHeight: 20, width: '100%' })
export const headTrack = style({ height: 20, width: '100%' })
