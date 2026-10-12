import { style } from '@vanilla-extract/css'
import { breakpoints } from '@/styles/breakpoints'
export const layout = style({
  display: 'flex',
  height: '100dvh',
  overflow: 'hidden',
  '@media': { [`screen and (max-width: ${breakpoints.mobile})`]: { flexDirection: 'column' } },
})
