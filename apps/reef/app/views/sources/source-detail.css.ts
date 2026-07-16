import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const header = style({
  alignItems: 'flex-start',
  display: 'flex',
  gap: 16,
  paddingBlockEnd: 8,
})

export const headerText = style({
  display: 'flex',
  flexDirection: 'column',
  flexGrow: 1,
  gap: 8,
  minWidth: 0,
})

export const headerTitleRow = style({
  alignItems: 'center',
  display: 'flex',
  gap: 10,
  marginInlineEnd: 24,
})

export const headerTitle = style({
  textTransform: 'capitalize',
})

export const section = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  paddingBlockStart: 16,
})

export const fieldGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
})

export const fieldItem = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
})

export const fieldLabel = style({
  color: theme.content.primary,
  fontWeight: 500,
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
