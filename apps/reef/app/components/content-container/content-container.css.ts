import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/breakpoints'
import { theme } from '@/wax/theme/theme.css'

export const contentContainer = style({
  backgroundColor: theme.surface.main,
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  minWidth: 0,
  minHeight: 0,
  overflow: 'auto',
})

// The page sits flush beside the sidebar, split from it by one line.
export const content = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      borderInlineStart: 0,
    },
  },
  background: theme.surface.mainContent,
  borderInlineStart: `1px solid ${theme.stroke.mainContent}`,
  flex: 1,
  maxHeight: '100dvh',
  minHeight: 0,
  overflow: 'hidden',
})
