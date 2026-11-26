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
/**
 * A dense row: one track, and nothing else for the eye. Its name, duration and context are
 * cells for a reader of the table, and its note shows them.
 */
export const denseRow = style({
  selectors: {
    // Over the table's own row rules, whatever order they load in.
    '&&&': {
      alignItems: 'center',
      borderBottomWidth: 0,
      columnGap: 0,
      minHeight: 16,
      paddingBlock: 0,
    },
  },
})
/** A cell only a reader of the table reads: the row's note shows it to the eye. */
export const readerCell = style({
  position: 'absolute',
  width: 1,
  height: 1,
  overflow: 'hidden',
  clipPath: 'inset(50%)',
  whiteSpace: 'nowrap',
})
/** The row's track, across the whole row: every track starts and ends where the others do. */
export const trackLine = style({
  gridColumn: '1 / -1',
  minWidth: 0,
  // Its note shows under the row, over the next one.
  selectors: { '&&&': { overflow: 'visible' } },
})
// The cell's own content box clips too: the note shows past it.
globalStyle(`${trackLine}${trackLine} > div`, { overflow: 'visible' })
/** A record's toggle and icon, before its bar: the label lives in the row's note instead. */
export const leadRecord = style({ display: 'flex' })
globalStyle(`${leadRecord} > div`, { paddingInlineStart: 0, width: 'auto' })
globalStyle(`${leadRecord} > div > div:last-child`, { display: 'none' })
/** Holds the track and the note under it, which the track would clip. */
export const trackBox = style({ position: 'relative', width: '100%' })
/** A row's note shows while you point at the row, the keys rest on it, or it is selected. */
export const rowNote = style({
  visibility: 'hidden',
  selectors: {
    '[role="row"]:hover &, [data-active] &, [aria-selected="true"] &': { visibility: 'visible' },
  },
})
/** A timeline cell: the track fills its line, so bars centre in it. */
export const trackCell = style({
  height: 16,
  width: '100%',
  // Bars past the view's ends stay clipped; a bar's ring and halo show above and below it.
  selectors: { '&&': { overflowX: 'clip', overflowY: 'visible' } },
})
export const headTrack = style({ height: 20, width: '100%' })
/** The overview above the table, for a Run at the root. The page sets its ends to the tracks'. */
export const overviewLine = style({ marginInline: 11, minWidth: 0 })
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
// The head's row is the axis alone: its other columns take no room, and no gap parts them.
globalStyle(`${foldsTable}${foldsTable} [role="row"]`, { columnGap: 0 })
// Bars a little taller than the table's, so those of one Run nearly touch.
globalStyle(`${trackCell} [data-timeline-bar]`, { height: 14 })
/** The first row of a Run: a line and some room part it from the Run before. */
export const runStart = style({})
globalStyle(`${foldsTable} [role="rowgroup"] > ${runStart}:not(:first-child)`, {
  marginTop: 6,
  boxShadow: `inset 0 1px 0 ${theme.stroke.primary}`,
  paddingTop: 2,
})
// A dense row shows its state on its bar, not across the row: the row keeps no fill and
// no outline when you point at it, select it, or the keys rest on it.
const denseRowState = [':hover', '[aria-selected="true"]', '[data-active]']
  .map((state) => `${denseRow}${denseRow}${denseRow}${denseRow}${state}`)
  .join(', ')
globalStyle(denseRowState, {
  backgroundColor: 'transparent',
  backgroundImage: 'none',
  boxShadow: 'none',
})
// A pinned heading stays opaque, so rows that scroll under it do not show through.
globalStyle(
  `${foldsTable} ${denseRow}${denseRow}${denseRow}${denseRow}[data-pinned-heading][aria-selected="true"]`,
  { backgroundColor: theme.surface.mainContent },
)
const mainBar = `[data-timeline-bar]:not([data-timeline-bar="wait"])`
// Pointing: a thin ring on the bar.
globalStyle(`${denseRow}:hover ${trackCell} ${mainBar}`, {
  outline: `1px solid ${theme.content.secondary}`,
  outlineOffset: 1,
  zIndex: 3,
})
// Selected: the timeline's own 2px ring, over its neighbours.
globalStyle(`${denseRow}[aria-selected="true"] ${trackCell} ${mainBar}`, { zIndex: 3 })
// The keys rest here: a halo, apart from the bar by a gap, in the focus colour. Its shape
// reads apart from the selected ring's, and it shows beside it on a selected bar.
globalStyle(`${denseRow}[data-active] ${trackCell} ${mainBar}`, {
  boxShadow: `0 0 0 3px ${theme.surface.mainContent}, 0 0 0 5px ${theme.stroke.focused}`,
  zIndex: 4,
})
// Forced colours drop the halo's shadow: an outline stands in for it.
globalStyle(`${denseRow}[data-active] ${trackCell} ${mainBar}`, {
  '@media': {
    '(forced-colors: active)': { outline: '2px solid CanvasText', outlineOffset: 3 },
  },
})
