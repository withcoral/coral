import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

export const page = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  overflow: 'auto',
  paddingBlock: '32px',
  paddingInline: '24px',
  '@media': {
    [MOBILE_QUERY]: {
      paddingBlock: '20px',
      paddingInline: '16px',
    },
  },
})

export const container = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '32px',
  marginInline: 'auto',
  maxWidth: '960px',
  width: '100%',
  '@media': {
    [MOBILE_QUERY]: {
      gap: '24px',
    },
  },
})
