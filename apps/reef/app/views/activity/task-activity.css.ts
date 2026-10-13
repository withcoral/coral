import { style } from '@vanilla-extract/css'

export const root = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  height: '100%',
  minHeight: 0,
  minWidth: 0,
  flex: 1,
})
export const toolbar = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  gap: 12,
  selectors: { '&:empty': { display: 'none' } },
})
export const filters = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  gap: 8,
  selectors: { '&:empty': { display: 'none' } },
})
export const stack = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  minWidth: 0,
})
export const list = style({
  display: 'flex',
  flexDirection: 'column',
  flex: '1 1 auto',
  minHeight: 0,
  minWidth: 0,
  gap: 16,
})
/** Sized by its rows; the history list below takes the remaining height. */
export const pending = style({
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  minWidth: 0,
  gap: 12,
})
export const metadata = style({ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 0 })
export const native = style({
  whiteSpace: 'pre-wrap',
  overflowWrap: 'anywhere',
  flexShrink: 0,
})

// One record's detail, keyed so it mounts per record; its parts stay the drawer's.
export const detail = style({ display: 'contents' })
