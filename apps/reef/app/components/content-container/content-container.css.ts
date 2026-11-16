import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/breakpoints'
import { theme } from '@/wax/theme/theme.css'

import { FRAME_INSET_PX } from './constants'

const CORNER_RADIUS_PX = 12
const MOBILE = `screen and (max-width: ${breakpoints.mobile})`

export const contentContainer = style({
  backgroundColor: theme.surface.main,
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  minWidth: 0,
  minHeight: 0,
  overflow: 'auto',
  // The sidebar's own padding is the gap on its side.
  padding: `${FRAME_INSET_PX}px ${FRAME_INSET_PX}px ${FRAME_INSET_PX}px 0`,
  '@media': { [MOBILE]: { padding: 0 } },
})

// The page is a frame beside the sidebar: a fine line and round corners set
// it a little inside the window's edge. A narrow window has no sidebar, so
// there the page fills it.
export const content = style({
  '@media': {
    [MOBILE]: { border: 0, borderRadius: 0, maxHeight: '100dvh' },
  },
  background: theme.surface.mainContent,
  border: `1px solid ${theme.stroke.mainContent}`,
  borderRadius: CORNER_RADIUS_PX,
  flex: 1,
  maxHeight: `calc(100dvh - ${2 * FRAME_INSET_PX}px)`,
  minHeight: 0,
  overflow: 'hidden',
})
