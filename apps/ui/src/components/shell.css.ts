import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme'
import { theme } from '@/wax/theme/theme.css'

const CONTENT_MARGIN = 12

export const root = style({
  backgroundColor: theme.surface.main,
  color: theme.content.primary,
  display: 'flex',
  height: '100dvh',
  overflow: 'hidden',
  width: '100vw',
})

export const mainArea = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  minWidth: 0,
})

export const content = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      borderRadius: 0,
      margin: 0,
      maxHeight: '100dvh',
    },
  },
  background: theme.surface.mainContent,
  border: `1px solid ${theme.stroke.mainContent}`,
  borderRadius: 8,
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  marginBlock: CONTENT_MARGIN,
  marginInlineEnd: CONTENT_MARGIN,
  maxHeight: `calc(100dvh - ${CONTENT_MARGIN * 2}px)`,
  minWidth: 0,
  overflow: 'hidden',
})
