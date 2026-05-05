import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const placeholder = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  justifyContent: 'center',
  padding: 32,
})

export const card = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: 16,
  boxShadow: theme.elevation.e1,
  color: theme.content.primary,
  display: 'flex',
  flexDirection: 'column',
  gap: 10,
  maxWidth: 420,
  padding: '28px 32px',
  textAlign: 'center',
})

export const title = style({
  color: theme.content.primary,
  fontFamily: theme.typography.headingSmall.fontFamily,
  fontSize: theme.typography.headingSmall.fontSize,
  fontWeight: theme.typography.headingSmall.fontWeight,
  letterSpacing: theme.typography.headingSmall.letterSpacing,
  lineHeight: theme.typography.headingSmall.lineHeight,
})

export const message = style({
  color: theme.content.secondary,
  fontFamily: theme.typography.body.fontFamily,
  fontSize: theme.typography.body.fontSize,
  fontWeight: theme.typography.body.fontWeight,
  letterSpacing: theme.typography.body.letterSpacing,
  lineHeight: theme.typography.body.lineHeight,
})
