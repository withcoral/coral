import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const container = style({
  alignItems: 'flex-start',
  backgroundColor: theme.surface.floating,
  border: `1px solid ${theme.stroke.floating}`,
  borderRadius: '10px',
  boxShadow: theme.elevation.e4,
  display: 'flex',
  gap: '10px',
  padding: '12px',
  position: 'relative',
  width: '324px',
})

export const iconWrapper = style({
  alignItems: 'flex-start',
  display: 'flex',
  flexShrink: 0,
  paddingTop: '2px',
})

export const content = style({
  alignItems: 'flex-start',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: '6px',
  minWidth: 0,
  paddingRight: '14px',
})

export const closeButton = style({
  position: 'absolute',
  right: '8px',
  top: '10px',
})
