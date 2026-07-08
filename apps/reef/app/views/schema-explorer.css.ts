import { style } from '@vanilla-extract/css'

import { pulse } from '@/wax/animations/pulse.css'
import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  minHeight: 0,
})

// Search field placed in the shared PageHeader's actions slot.
export const headerSearch = style({
  width: 280,
  maxWidth: '40vw',
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

export const treePanel = style({
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

export const treeContent = style({
  flex: 1,
  minHeight: 0,
})

export const treeList = style({
  padding: 4,
  paddingInlineEnd: 6,
})

export const treeEmpty = style({
  padding: 12,
  textAlign: 'center',
})

export const treeError = style({
  padding: 12,
})

export const skeletonContainer = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  padding: 12,
})

export const skeletonGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
})

export const skeletonChildren = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  marginInlineStart: 18,
})

// Tree rows render as bare wax buttons (Button.Container variant="bare"); the
// layout differs from the button default (left-aligned, icon/label gap), and the
// bare variant has no hover background, so add the tree hover here. `:not(:disabled)`
// bumps specificity above the button's own `:hover` rule so this reliably wins.
export const treeRow = style({
  borderRadius: 4,
  gap: 6,
  justifyContent: 'flex-start',
  selectors: {
    '&:hover:not(:disabled)': {
      background: theme.surface.onMainContentSubtle,
    },
  },
})

export const connectorName = style({
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const connectorTableCount = style({
  color: theme.content.tertiary,
  marginInlineStart: 'auto',
})

export const connectorChildren = style({
  display: 'flex',
  flexDirection: 'column',
  marginInlineStart: 16,
})

export const tableName = style({
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const detailPanel = style({
  flex: 1,
  minWidth: 0,
  overflow: 'auto',
})

export const detailContent = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 18,
  padding: 18,
})

export const detailHeader = style({
  alignItems: 'flex-start',
  display: 'flex',
  gap: 12,
  justifyContent: 'space-between',
  minWidth: 0,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      flexDirection: 'column',
    },
  },
})

export const description = style({
  color: theme.content.secondary,
  lineHeight: '20px',
  marginBlockEnd: 0,
  marginBlockStart: 4,
  maxWidth: 840,
})

export const requiredFilterGroup = style({
  alignItems: 'center',
  display: 'flex',
  flexShrink: 0,
  flexWrap: 'wrap',
  gap: 6,
  justifyContent: 'flex-end',
  maxWidth: '40%',
  outlineOffset: 2,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      justifyContent: 'flex-start',
      maxWidth: '100%',
    },
  },
})

export const section = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
})

export const virtualRow = style({
  fontStyle: 'italic',
})

export const cellTruncate = style({
  maxWidth: 360,
})

export const requiredStar = style({
  color: theme.content.error,
  cursor: 'help',
  display: 'inline-flex',
  font: 'inherit',
  marginInlineStart: 4,
  outlineOffset: 2,
})

export const loadingState = style({
  ...pulse,
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  minHeight: 44,
})

export const emptyInline = style({
  display: 'block',
  paddingBlock: 8,
})

export const detailEmpty = style({
  alignItems: 'center',
  display: 'flex',
  height: '100%',
  justifyContent: 'center',
  padding: 24,
  textAlign: 'center',
})
