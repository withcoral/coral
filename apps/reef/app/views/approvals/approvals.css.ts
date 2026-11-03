import { style } from '@vanilla-extract/css'

/** The list beside its docked review. Each scrolls on its own. */
export const page = style({ display: 'flex', height: '100%', minHeight: 0, minWidth: 0 })
export const list = style({
  display: 'flex',
  flex: '1 1 0',
  flexDirection: 'column',
  gap: 16,
  minWidth: 0,
  overflowY: 'auto',
})

/** The request beside its actions, which wrap under it when the row has no room. */
export const row = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 12,
})
export const request = style({
  display: 'flex',
  flex: '1 1 320px',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})
export const actions = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  gap: 12,
})
/** The call's provider logo, tool and deadline on one line, which wraps between them. */
export const meta = style({
  display: 'inline-flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  columnGap: 4,
})
/** The history fills what the pending requests leave, and stays tall enough to read when
 * many requests wait: the list then scrolls them both. */
export const history = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  minHeight: 280,
  minWidth: 0,
})
