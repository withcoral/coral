import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const header = style({
  alignItems: 'flex-start',
  display: 'flex',
  gap: 12,
})

export const headerLogo = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  borderRadius: '50%',
  display: 'flex',
  flexShrink: 0,
  height: 40,
  justifyContent: 'center',
  overflow: 'hidden',
  width: 40,
})

export const headerLogoImg = style({
  height: '100%',
  objectFit: 'cover',
  width: '100%',
})

export const headerText = style({
  display: 'flex',
  flexDirection: 'column',
  flexGrow: 1,
  gap: 4,
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

export const headerPill = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 999,
  color: theme.content.secondary,
  display: 'inline-flex',
  fontSize: 11,
  fontWeight: 600,
  letterSpacing: '0.02em',
  padding: '2px 8px',
  textTransform: 'uppercase',
})

export const section = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
})

export const bindingList = style({
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  padding: 12,
})

export const bindingRow = style({
  alignItems: 'center',
  display: 'grid',
  gap: 10,
  gridTemplateColumns: 'minmax(120px, max-content) 1fr',
})

export const keyLabel = style({
  color: theme.content.secondary,
  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
  fontSize: 12,
})

export const alertError = style({
  alignItems: 'center',
  background: theme.pill.red.background,
  border: `1px solid ${theme.pill.red.stroke}`,
  borderRadius: 6,
  color: theme.pill.red.color,
  display: 'flex',
  fontSize: 12,
  gap: 8,
  lineHeight: '16px',
  paddingBlock: 8,
  paddingInline: 12,
})
