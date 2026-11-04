import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

/** What a root produced: one card per function it saved. */
export const outputs = style({ display: 'flex', flexDirection: 'column', gap: 6 })
export const cards = style({
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
  gap: 8,
})
/** One saved function: its name in full, over two lines at most, and when it was saved. */
export const card = style({
  display: 'flex',
  alignItems: 'flex-start',
  gap: 8,
  boxSizing: 'border-box',
  width: '100%',
  height: '100%',
  padding: '8px 10px',
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 6,
  selectors: {
    '&[aria-current="true"]': { borderColor: theme.stroke.primary },
  },
})
export const cardBody = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  flex: 1,
  minWidth: 0,
})
export const cardName = style({
  display: 'flex',
  alignItems: 'center',
  gap: 4,
  minWidth: 0,
  overflow: 'hidden',
  whiteSpace: 'nowrap',
  textOverflow: 'ellipsis',
  textDecoration: 'none',
  selectors: { '&:hover': { textDecoration: 'underline' } },
})
/** "Smoke-tested", with its check mark beside it. */
export const tested = style({ display: 'inline-flex', alignItems: 'center', gap: 2 })
