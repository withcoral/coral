import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  minHeight: 0,
})

export const body = style({
  display: 'flex',
  flex: 1,
  minHeight: 0,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      flexDirection: 'column',
    },
  },
})

export const listPanel = style({
  borderInlineEnd: `1px solid ${theme.stroke.primary}`,
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  minHeight: 0,
  width: 360,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      borderBlockEnd: `1px solid ${theme.stroke.primary}`,
      borderInlineEnd: 0,
      maxHeight: '40%',
      width: '100%',
    },
  },
})

export const listContent = style({
  flex: 1,
  minHeight: 0,
})

export const list = style({
  padding: 4,
  paddingInlineEnd: 6,
})

export const listRow = style({
  borderRadius: 4,
  justifyContent: 'flex-start',
})

export const functionName = style({
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const listEmpty = style({
  padding: 12,
  textAlign: 'center',
})

export const detailPanel = style({
  display: 'flex',
  flex: 1,
  minWidth: 0,
  overflow: 'hidden',
})

export const detailEmpty = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  justifyContent: 'center',
  padding: 24,
  textAlign: 'center',
})
