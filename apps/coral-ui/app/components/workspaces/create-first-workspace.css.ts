import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

export const page = style({
  alignItems: 'center',
  backgroundColor: theme.surface.mainContent,
  boxSizing: 'border-box',
  display: 'flex',
  justifyContent: 'center',
  minHeight: '100dvh',
  padding: 24,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      alignItems: 'flex-start',
      padding: 16,
    },
  },
})

export const card = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 12,
  boxShadow: theme.elevation.e2,
  boxSizing: 'border-box',
  display: 'flex',
  flexDirection: 'column',
  gap: 20,
  maxWidth: 440,
  padding: 28,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      padding: 20,
    },
  },
})

export const intro = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
})

export const form = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
})

export const field = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
})
