import { keyframes, style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme'
import { theme, zIndex } from '@/wax/theme/theme.css'

const CONTENT_MARGIN = 12
const navigationProgressSweep = keyframes({
  '0%': {
    transform: 'translateX(-70%) scaleX(0.35)',
  },
  '55%': {
    transform: 'translateX(12%) scaleX(0.9)',
  },
  '100%': {
    transform: 'translateX(115%) scaleX(0.45)',
  },
})

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
  position: 'relative',
})

export const navigationProgress = style({
  background: theme.surface.skeleton,
  height: 3,
  insetBlockStart: 0,
  insetInline: 0,
  overflow: 'hidden',
  pointerEvents: 'none',
  position: 'absolute',
  selectors: {
    '&::before': {
      animation: `${navigationProgressSweep} 900ms ease-in-out infinite`,
      background: theme.content.info,
      content: '""',
      height: '100%',
      insetBlockStart: 0,
      insetInlineStart: 0,
      position: 'absolute',
      transformOrigin: 'left center',
      width: '65%',
    },
  },
  width: '100%',
  zIndex: zIndex.raised,
  '@media': {
    '(prefers-reduced-motion: reduce)': {
      selectors: {
        '&::before': {
          animation: 'none',
          transform: 'none',
          width: '100%',
        },
      },
    },
  },
})
