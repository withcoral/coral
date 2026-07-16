import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const fieldGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  paddingBlockStart: 16,
})

export const alertError = style({
  alignItems: 'center',
  background: theme.pill.red.background,
  border: `1px solid ${theme.pill.red.stroke}`,
  borderRadius: 6,
  color: theme.pill.red.color,
  display: 'flex',
  fontSize: 12,
  gap: 8,
  lineHeight: '16px',
  paddingBlock: 8,
  paddingInline: 12,
})

export const removeConfirmText = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})

export const removeConfirmActions = style({
  display: 'flex',
  flexShrink: 0,
  gap: 10,
  justifyContent: 'flex-end',
})
