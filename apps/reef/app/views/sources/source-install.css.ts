import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const fieldGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  paddingBlockStart: 16,
})

export const methodTabsRoot = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
})

export const methodTabs = style({
  paddingInline: 0,
  scrollPaddingInline: 0,
})

export const methodPanels = style({
  display: 'grid',
})

export const methodPanel = style({
  gridArea: '1 / 1',
})

export const methodSizer = style([methodPanel, { visibility: 'hidden' }])

export const methodPanelContent = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
})

export const alertBox = style({
  alignItems: 'center',
  borderRadius: 6,
  display: 'flex',
  fontSize: 12,
  gap: 8,
  lineHeight: '16px',
  paddingBlock: 8,
  paddingInline: 12,
})

export const alertError = style({
  background: theme.pill.red.background,
  border: `1px solid ${theme.pill.red.stroke}`,
  color: theme.pill.red.color,
})
