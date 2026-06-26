import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

const CONTAINER_MARGIN_PX = 12

export const contentContainer = style({
  backgroundColor: theme.surface.main,
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  minWidth: 0,
  overflow: 'auto',
})

export const content = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      borderRadius: 0,
      marginBlock: 0,
      marginInlineEnd: 0,
      maxHeight: '100dvh',
    },
  },
  background: theme.surface.mainContent,
  border: `1px solid ${theme.stroke.mainContent}`,
  borderRadius: '8px',
  flex: 1,
  marginBlock: `${CONTAINER_MARGIN_PX}px`,
  marginInlineEnd: `${CONTAINER_MARGIN_PX}px`,
  maxHeight: `calc(100dvh - ${CONTAINER_MARGIN_PX * 2}px)`,
  minHeight: 0,
  overflow: 'hidden',
})
