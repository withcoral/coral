import { style } from '@vanilla-extract/css'

import { fontFamily } from '@/wax/theme/font.css'
import { animation, theme } from '@/wax/theme/theme.css'

export const wrapperCompact = style({
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: 6,
  overflowX: 'auto',
})

export const wrapperDefault = style({
  overflowX: 'auto',
})

export const table = style({
  borderCollapse: 'collapse',
  fontFamily: fontFamily.encodeSans,
  fontSize: '12px',
  lineHeight: '16px',
  width: '100%',
})

export const thead = style({
  backgroundColor: theme.surface.card,
  position: 'sticky',
  top: 0,
  zIndex: 1,
})

export const th = style({
  borderBottom: `1px solid ${theme.stroke.primary}`,
  color: theme.content.secondary,
  fontWeight: 500,
  paddingBlock: 6,
  paddingInline: 12,
  textAlign: 'left',
  whiteSpace: 'nowrap',
  selectors: {
    [`${wrapperDefault} &`]: {
      borderTop: `1px solid ${theme.stroke.primary}`,
      paddingBlock: 10,
      paddingInline: 8,
    },
    [`${wrapperDefault} &:first-child`]: { paddingInlineStart: 32 },
    [`${wrapperDefault} &:last-child`]: { paddingInlineEnd: 32 },
  },
})

export const tbody = style({})

export const tr = style({
  borderBottom: `1px solid ${theme.stroke.primary}`,
  transition: animation.colorTransition,
  selectors: {
    'tbody &:hover': {
      backgroundColor: theme.surface.onMainContentSubtle,
    },
    [`${wrapperDefault} tbody &:hover`]: {
      backgroundColor: theme.surface.onMainContent,
    },
    '&:last-child': {
      borderBottom: 0,
    },
  },
})

export const td = style({
  fontFamily: fontFamily.dmMono,
  maxWidth: 280,
  overflow: 'hidden',
  paddingBlock: 5,
  paddingInline: 12,
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  selectors: {
    [`${wrapperDefault} &`]: {
      paddingBlock: 8,
      paddingInline: 8,
    },
    [`${wrapperDefault} &:first-child`]: { paddingInlineStart: 32 },
    [`${wrapperDefault} &:last-child`]: { paddingInlineEnd: 32 },
  },
})

export const tdText = style({
  maxWidth: 320,
  overflow: 'hidden',
  paddingBlock: 5,
  paddingInline: 12,
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  selectors: {
    [`${wrapperDefault} &`]: {
      paddingBlock: 8,
      paddingInline: 8,
    },
    [`${wrapperDefault} &:first-child`]: { paddingInlineStart: 32 },
    [`${wrapperDefault} &:last-child`]: { paddingInlineEnd: 32 },
  },
})
