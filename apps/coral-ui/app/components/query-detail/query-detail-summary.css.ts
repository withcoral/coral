import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  height: '100%',
  minHeight: 0,
})

export const header = style({
  alignItems: 'center',
  borderBlockEnd: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexShrink: 0,
  height: 56,
  justifyContent: 'space-between',
  overflow: 'hidden',
  paddingBlock: 6,
  paddingInline: 32,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      paddingInline: 16,
    },
  },
})

export const headerTitle = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  gap: 4,
  minWidth: 0,
  overflow: 'hidden',
  paddingInlineEnd: 24,
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const headerActions = style({
  alignItems: 'center',
  display: 'flex',
  flexShrink: 0,
  gap: 4,
})

export const statusBadge = style({
  borderRadius: 999,
  display: 'inline-flex',
  fontSize: 12,
  lineHeight: '18px',
  paddingBlock: 2,
  paddingInline: 8,
  selectors: {
    '&[data-tone="ok"]': {
      backgroundColor: theme.pill.green.background,
      color: theme.pill.green.color,
    },
    '&[data-tone="error"]': {
      backgroundColor: theme.pill.red.background,
      color: theme.pill.red.color,
    },
    '&[data-tone="running"]': {
      backgroundColor: theme.pill.blue.background,
      color: theme.pill.blue.color,
    },
  },
})

export const scrollBody = style({
  flex: 1,
  minHeight: 0,
})

export const content = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 16,
  minHeight: '100%',
  padding: 16,
})

export const statGrid = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
})

export const statCard = style({
  backgroundColor: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 12,
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  minWidth: 100,
  paddingBlock: 12,
  paddingInline: 16,
})
