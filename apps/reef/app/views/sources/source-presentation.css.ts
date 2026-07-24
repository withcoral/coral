import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const error = style({
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

export const headerIdentity = style({
  alignItems: 'baseline',
  display: 'inline-flex',
  gap: 10,
})

export const headerTitle = style({
  textTransform: 'capitalize',
})

export const fieldItem = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
})

export const noConfiguration = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  paddingBlockStart: 16,
})
