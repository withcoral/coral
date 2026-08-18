import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

export const page = style({
  height: '100%',
  overflow: 'auto',
})

// The padding sits inside the scroll port rather than on it. A sticky heading
// takes `top: 0` from the port's padding edge, so padding on the port itself pins
// the heading that far down the pane and lets rows scroll through the gap.
export const pageContent = style({
  display: 'flex',
  flexDirection: 'column',
  paddingBlock: '32px',
  paddingInline: '24px',
  '@media': {
    [MOBILE_QUERY]: {
      paddingBlock: '20px',
      paddingInline: '16px',
    },
  },
})

export const section = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '24px',
})

export const sectionHeader = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '4px',
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
