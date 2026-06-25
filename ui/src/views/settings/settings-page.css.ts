import { keyframes, style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

const spin = keyframes({
  from: { transform: 'rotate(0deg)' },
  to: { transform: 'rotate(360deg)' },
})

export const spinAnimation = style({
  animation: `${spin} 1s linear infinite`,
})

export const root = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  overflow: 'auto',
  paddingBlock: 32,
  paddingInline: 24,
})

export const container = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 24,
  marginInline: 'auto',
  maxWidth: 880,
  width: '100%',
})

export const header = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
})

export const section = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  overflow: 'hidden',
})

export const sectionHeader = style({
  borderBlockEnd: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  paddingBlock: 16,
  paddingInline: 18,
})

export const row = style({
  alignItems: 'center',
  display: 'grid',
  gap: 16,
  gridTemplateColumns: 'minmax(0, 1fr) auto',
  minHeight: 78,
  paddingBlock: 16,
  paddingInline: 18,
  selectors: {
    '& + &': {
      borderBlockStart: `1px solid ${theme.stroke.secondary}`,
    },
  },
})

export const rowMain = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 7,
  minWidth: 0,
})

export const inlineTitle = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  minWidth: 0,
})

export const code = style({
  background: theme.content.code.inlineBackground,
  borderRadius: 5,
  color: theme.content.code.inlineColor,
  fontFamily: theme.typography.codeInline.fontFamily,
  fontSize: theme.typography.codeInline.fontSize,
  fontWeight: theme.typography.codeInline.fontWeight,
  lineHeight: theme.typography.codeInline.lineHeight,
  paddingBlock: 2,
  paddingInline: 6,
})

export const meta = style({
  color: theme.content.tertiary,
  fontFamily: theme.typography.codeSmallInline.fontFamily,
  fontSize: theme.typography.codeSmallInline.fontSize,
  fontWeight: theme.typography.codeSmallInline.fontWeight,
  lineHeight: theme.typography.codeSmallInline.lineHeight,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const success = style({
  color: theme.content.success,
})

export const error = style({
  color: theme.content.error,
  whiteSpace: 'normal',
})

export const unavailable = style({
  alignItems: 'center',
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  paddingBlock: 48,
  paddingInline: 24,
  textAlign: 'center',
})

export const actionSlot = style({
  alignItems: 'center',
  display: 'flex',
  justifyContent: 'flex-end',
})
