import { style } from '@vanilla-extract/css'

import { utils } from '@/styles/utils'
import { theme } from '@/wax/theme/theme.css'

export const card = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '12px',
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
  padding: '16px',
  textDecoration: 'none',
  width: '100%',
})

export const cardButton = style({
  alignItems: 'stretch',
  color: 'inherit',
  cursor: 'pointer',
  font: 'inherit',
  justifyContent: 'flex-start',
  textAlign: 'left',
  selectors: {
    '&:hover': {
      background: theme.surface.onMainContentHover,
    },
    '&:focus-visible': {
      outline: `2px solid ${theme.stroke.focused}`,
      outlineOffset: '2px',
    },
  },
})

export const header = style({
  alignItems: 'center',
  display: 'flex',
  gap: '10px',
  minWidth: 0,
})

export const title = style({
  flexShrink: 1,
  minWidth: 0,
  textTransform: 'capitalize',
})

export const headerPill = style({
  flexShrink: 0,
  marginLeft: 'auto',
})

export const description = style({
  ...utils.boxClamp(5),
})
