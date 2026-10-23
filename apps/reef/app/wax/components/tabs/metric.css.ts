import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

// The tiles share the card's top edge; the chosen one carries a rule along its foot.
export const metricList = style({
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
  borderBottom: `1px solid ${theme.stroke.secondary}`,
})

export const metricTab = style({
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'flex-start',
  gap: 4,
  minWidth: 0,
  padding: '14px 16px 12px',
  border: 0,
  background: 'transparent',
  color: 'inherit',
  font: 'inherit',
  textAlign: 'left',
  cursor: 'pointer',
  boxShadow: 'inset 0 -2px 0 transparent',
  ':hover': { background: theme.button.bare.hover },
  ':focus-visible': { outline: `2px solid ${theme.input.stroke.focus}`, outlineOffset: -2 },
  selectors: {
    '&[aria-selected="true"]': { boxShadow: `inset 0 -2px 0 ${theme.content.primary}` },
    '& + &': { borderLeft: `1px solid ${theme.stroke.secondary}` },
  },
})

export const metricLabel = style({ display: 'inline-flex', alignItems: 'center', gap: 4 })

export const metricDetail = style({
  maxWidth: '100%',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
