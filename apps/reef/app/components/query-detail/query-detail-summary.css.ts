import { globalStyle, style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { fontFamily } from '@/wax/theme/font.css'
import { lightTheme } from '@/wax/theme/theme-light.css'
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
  display: 'flex',
  flex: 1,
  minHeight: 0,
  overflow: 'auto',
})

export const content = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 16,
  minHeight: '100%',
  padding: 16,
})

export const sqlBlock = style({
  backgroundColor: theme.surface.main,
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: 8,
  overflow: 'hidden',
  position: 'relative',
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

globalStyle(`${sqlBlock} pre`, {
  color: theme.content.primary,
  fontFamily: fontFamily.dmMono,
  fontSize: 14,
  lineHeight: 1.65,
  margin: 0,
  overflowX: 'auto',
  padding: 12,
  whiteSpace: 'pre-wrap',
  wordBreak: 'break-all',
})

globalStyle(`${sqlBlock} .sql-keyword`, { color: '#569CD6', fontWeight: 600 })
globalStyle(`body.${lightTheme} ${sqlBlock} .sql-keyword`, { color: '#0000FF' })
globalStyle(`${sqlBlock} .sql-function`, { color: '#4EC9B0' })
globalStyle(`body.${lightTheme} ${sqlBlock} .sql-function`, { color: '#795E26' })
globalStyle(`${sqlBlock} .sql-string`, { color: '#CE9178' })
globalStyle(`body.${lightTheme} ${sqlBlock} .sql-string`, { color: '#A31515' })
globalStyle(`${sqlBlock} .sql-number`, { color: '#CE9178' })
globalStyle(`body.${lightTheme} ${sqlBlock} .sql-number`, { color: '#098658' })
globalStyle(`${sqlBlock} .sql-comment`, { color: '#6A9955', fontStyle: 'italic' })
globalStyle(`body.${lightTheme} ${sqlBlock} .sql-comment`, { color: '#008000' })
globalStyle(`${sqlBlock} .sql-identifier`, { color: '#9CDCFE' })
globalStyle(`body.${lightTheme} ${sqlBlock} .sql-identifier`, { color: '#001080' })
