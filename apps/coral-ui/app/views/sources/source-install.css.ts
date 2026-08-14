import { style } from '@vanilla-extract/css'

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
