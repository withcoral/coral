import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const header = style({
  alignItems: 'center',
  borderBlockEnd: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexShrink: 0,
  gap: 12,
  paddingBlock: 16,
  paddingInline: 32,
})

export const leading = style({
  alignItems: 'center',
  display: 'flex',
  flexShrink: 0,
})

export const titleArea = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 2,
  minWidth: 0,
})

export const actions = style({
  alignItems: 'center',
  display: 'flex',
  flexShrink: 0,
  gap: 8,
})
