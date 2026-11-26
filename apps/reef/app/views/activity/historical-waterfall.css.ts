// Story-only snapshot from tree bb9d070cbfde54b64dc49d9db811d6e6487ca85b.
// Historical comparison; not used by the production Activity page.
import { globalStyle, style } from '@vanilla-extract/css'
import { foldsBand, foldsBandInset, foldsImage } from '@/wax/components/timeline'
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
/** A row of two lines of set heights, so a call heading the calls under it pins exactly. */
export const twoLineRow = style({ gridTemplateRows: '24px 20px' })
/** A row's first line: its record takes its own column and the timeline's, which has no width. */
export const recordCell = style({ gridColumn: '1 / 3', gridRow: 1, minWidth: 0 })
/**
 * A row's second line: its track, across the whole row. Every track starts and ends where
 * the others do, however deep its record sits, and where the overview and the axis do.
 */
export const trackLine = style({ gridColumn: '1 / -1', gridRow: 2, minWidth: 0 })
/** A timeline cell: the track fills its line, so bars centre in it. */
export const trackCell = style({ alignSelf: 'stretch', height: 20, width: '100%' })
export const headTrack = style({ height: 20, width: '100%' })
/** The overview above the table, for a Run at the root. The page sets its ends to the tracks'. */
export const overviewLine = style({ marginInline: 11, minWidth: 0 })
/** The axis, inset as the tracks are. The page measures and sets both ends, as for the overview. */
export const axisLine = style({ marginInlineStart: 13, marginInlineEnd: 13, minWidth: 0 })
/**
 * The table of tracks shades the folds as one subtle band behind its rows, at the tracks'
 * span: inside the rows' padding.
 */
export const foldsTable = style([foldsBand, { vars: { [foldsBandInset]: '12px' } }])
// A pinned heading is opaque, as rows scroll under it: it draws the band too, so the band
// runs on through it. A selected one keeps its selection tint instead.
globalStyle(`${foldsTable} [data-pinned-heading]:not([aria-selected="true"])`, {
  backgroundImage: foldsImage,
  backgroundPosition: '12px 0',
  backgroundRepeat: 'no-repeat',
  backgroundSize: 'calc(100% - 24px) 100%',
})
