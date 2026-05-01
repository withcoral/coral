import { style } from '@vanilla-extract/css'
import { breakpoints } from '@/styles/theme'
import { theme } from '@/wax/theme/theme.css'

const CONTENT_MARGIN = 12

export const root = style({
  display: 'flex',
  height: '100dvh',
  width: '100vw',
  overflow: 'hidden',
  backgroundColor: theme.surface.main,
  color: theme.content.primary,
})

export const mainArea = style({
  display: 'flex',
  flexDirection: 'column',
  flex: 1,
  minWidth: 0,
})

export const content = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      margin: 0,
      borderRadius: 0,
      maxHeight: '100dvh',
    },
  },
  flex: 1,
  marginBlock: CONTENT_MARGIN,
  marginInlineEnd: CONTENT_MARGIN,
  maxHeight: `calc(100dvh - ${CONTENT_MARGIN * 2}px)`,
  background: theme.surface.mainContent,
  border: `1px solid ${theme.stroke.mainContent}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  overflow: 'hidden',
})
