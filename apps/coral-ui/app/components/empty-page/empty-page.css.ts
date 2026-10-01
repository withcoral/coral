import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'

export const fullSizeWrapper = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  justifyContent: 'center',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      padding: '24px 16px',
    },
  },
})

export const container = style({
  alignItems: 'center',
  display: 'flex',
  flexDirection: 'column',
  gap: 20,
  maxWidth: 400,
  textAlign: 'center',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gap: 16,
      maxWidth: '100%',
    },
  },
})

export const typographyContainer = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
})
