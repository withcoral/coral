import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/breakpoints'
import { theme } from '@/wax/theme/theme.css'

const CORNER_RADIUS_PX = 12

export const contentContainer = style({
  backgroundColor: theme.surface.main,
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  minWidth: 0,
  minHeight: 0,
  overflow: 'auto',
})

// The page sits flush beside the sidebar, split from it by one line. Its
// corners on the sidebar's side round off where they meet the window's edges.
export const content = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      borderInlineStart: 0,
      borderEndStartRadius: 0,
      borderStartStartRadius: 0,
    },
  },
  background: theme.surface.mainContent,
  borderEndStartRadius: CORNER_RADIUS_PX,
  borderInlineStart: `1px solid ${theme.stroke.mainContent}`,
  borderStartStartRadius: CORNER_RADIUS_PX,
  flex: 1,
  maxHeight: '100dvh',
  minHeight: 0,
  overflow: 'hidden',
})
