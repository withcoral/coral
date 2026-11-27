import { style } from '@/wax/css'
import { breakpoints } from '@/wax/styles/breakpoints'
import { theme } from '@/wax/theme/theme.css'

import { FRAME_INSET_PX, FRAME_LINE_PX } from './constants'

const CORNER_RADIUS_PX = 12
const MOBILE = `screen and (max-width: ${breakpoints.mobile})`

export const layout = style({
  backgroundColor: theme.surface.main,
  display: 'flex',
  height: '100dvh',
  overflow: 'hidden',
})

export const main = style({
  backgroundColor: theme.surface.main,
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  minWidth: 0,
  minHeight: 0,
  overflow: 'auto',
  // The navigation's own padding is the gap on its side.
  padding: `${FRAME_INSET_PX}px ${FRAME_INSET_PX}px ${FRAME_INSET_PX}px 0`,
  '@media': { [MOBILE]: { padding: 0 } },
})

// The page is a frame beside the navigation: a fine line and round corners set
// it a little inside the window's edge. A narrow window has no navigation
// beside the page, so there the page fills it.
export const page = style({
  '@media': {
    [MOBILE]: { border: 0, borderRadius: 0, maxHeight: '100dvh' },
  },
  background: theme.surface.mainContent,
  border: `${FRAME_LINE_PX}px solid ${theme.stroke.mainContent}`,
  borderRadius: CORNER_RADIUS_PX,
  flex: 1,
  maxHeight: `calc(100dvh - ${2 * FRAME_INSET_PX}px)`,
  minHeight: 0,
  overflow: 'hidden',
})
