import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

// — Compact (default): flush, no container chrome —

export const wrapper = style({
  overflowX: 'auto',
  borderRadius: '8px',
  border: `1px solid ${theme.stroke.primary}`,
})

export const wrapperCompact = style({
  overflowX: 'auto',
  borderRadius: 0,
  border: 'none',
})

export const wrapperDefault = style({
  overflowX: 'auto',
  borderRadius: 0,
  border: 'none',
})

export const table = style({
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: '12px',
  lineHeight: '16px',
  fontFamily: "'Gustan', sans-serif",
})

export const thead = style({
  position: 'sticky',
  top: 0,
  zIndex: 1,
  backgroundColor: theme.surface.card,
})

export const th = style({
  paddingInline: '12px',
  paddingBlock: '6px',
  textAlign: 'left',
  fontWeight: 500,
  color: theme.content.secondary,
  whiteSpace: 'nowrap',
  borderBottom: `1px solid ${theme.stroke.primary}`,
})

export const thDefault = style({
  paddingInline: '8px',
  paddingBlock: '10px',
  textAlign: 'left',
  fontWeight: 500,
  color: theme.content.secondary,
  whiteSpace: 'nowrap',
  borderBottom: `1px solid ${theme.stroke.primary}`,
  borderTop: `1px solid ${theme.stroke.primary}`,
  selectors: {
    '&:first-child': { paddingInlineStart: '32px' },
    '&:last-child': { paddingInlineEnd: '32px' },
  },
})

export const tbody = style({})

export const tr = style({
  borderBottom: `1px solid ${theme.stroke.primary}`,
  transition: 'background-color 0.1s ease',
  selectors: {
    'tbody &:hover': {
      backgroundColor: theme.surface.onMainContentSubtle,
    },
  },
})

export const trDefault = style({
  borderBottom: `1px solid ${theme.stroke.primary}`,
  transition: 'background-color 0.2s ease',
  selectors: {
    'tbody &:hover': {
      backgroundColor: theme.surface.onMainContent,
    },
  },
})

export const td = style({
  paddingInline: '12px',
  paddingBlock: '4px',
  fontFamily: "'Gustan Mono', monospace",
  maxWidth: '250px',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const tdDefault = style({
  paddingInline: '8px',
  paddingBlock: '8px',
  fontFamily: "'Gustan Mono', monospace",
  maxWidth: '250px',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  selectors: {
    '&:first-child': { paddingInlineStart: '32px' },
    '&:last-child': { paddingInlineEnd: '32px' },
  },
})

export const tdText = style({
  paddingInline: '12px',
  paddingBlock: '4px',
  maxWidth: '250px',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const tdTextDefault = style({
  paddingInline: '8px',
  paddingBlock: '8px',
  maxWidth: '250px',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  selectors: {
    '&:first-child': { paddingInlineStart: '32px' },
    '&:last-child': { paddingInlineEnd: '32px' },
  },
})
