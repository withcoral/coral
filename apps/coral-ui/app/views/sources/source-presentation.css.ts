import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const error = style({
  alignItems: 'flex-start',
  background: theme.pill.red.background,
  border: `1px solid ${theme.pill.red.stroke}`,
  borderRadius: 6,
  color: theme.pill.red.color,
  display: 'flex',
  gap: 8,
  paddingBlock: 8,
  paddingInline: 12,
})

// Coral renders errors as newline-separated summary / detail / `Hint:` lines, so
// keep the server's line structure and break long type paths inside the banner.
export const errorText = style({
  minWidth: 0,
  overflowWrap: 'anywhere',
  whiteSpace: 'pre-wrap',
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
