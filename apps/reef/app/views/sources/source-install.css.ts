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

export const fieldGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  paddingBlockStart: 16,
})

export const fieldItem = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
})

// Kept so the public API of <Field> is unchanged even though all fields are
// now full-width — selecting this className is a no-op against the flex
// column layout above.
export const fieldItemFull = style({})

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
