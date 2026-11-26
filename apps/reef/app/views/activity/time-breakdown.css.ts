import { style } from '@vanilla-extract/css'

/** What the breakdown says: its parts and its notes, or why no value shows. */
export const content = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
  paddingBlock: 8,
})
/** One line per part: its name, its value, its share, and a bar of that share. */
export const parts = style({
  display: 'grid',
  gridTemplateColumns: 'minmax(120px, max-content) max-content max-content minmax(40px, 1fr)',
  columnGap: 16,
  rowGap: 4,
  alignItems: 'center',
  margin: 0,
  padding: 0,
  listStyle: 'none',
})
export const part = style({ display: 'contents' })
/** A part's name with the control that explains it. */
export const label = style({ display: 'inline-flex', alignItems: 'center', gap: 4 })
/** A line that holds its room, unseen, until it shows. */
export const placeholder = style({ visibility: 'hidden' })
export const number = style({ textAlign: 'end', fontVariantNumeric: 'tabular-nums' })
/** A value's placeholder: as tall as the value, at its end of the column. */
export const pendingNumber = style({ display: 'flex', justifyContent: 'flex-end' })
