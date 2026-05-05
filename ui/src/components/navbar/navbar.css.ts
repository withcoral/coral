import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme'
import { theme, zIndex } from '@/wax/theme/theme.css'

const MAIN_CONTENT_PADDING = 12

export const navbar = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      minWidth: '58px',
    },
  },
  backgroundColor: theme.surface.main,
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  height: '100dvh',
  minWidth: '180px',
  padding: MAIN_CONTENT_PADDING,
  zIndex: zIndex.navigation,
})

export const header = style({
  alignItems: 'center',
  display: 'flex',
  gap: '8px',
  minHeight: '32px',
  paddingBlockStart: '10px',
  paddingInline: '8px',
})

export const brandName = style({
  color: theme.content.primary,
  fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
  fontSize: 15,
  fontWeight: 500,
  lineHeight: '145%',
})

export const emptyNav = style({
  flex: 1,
  marginBlockStart: '24px',
})
