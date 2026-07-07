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
  height: '100%',
})

export const scrollContent = style({
  paddingBlock: 32,
  paddingInline: 24,
})

export const container = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 32,
  marginInline: 'auto',
  maxWidth: 960,
  width: '100%',
})

export const header = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
})

export const loadingState = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  justifyContent: 'center',
  paddingBlock: 48,
})

export const emptyState = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 12,
  justifyContent: 'center',
  paddingBlock: 48,
  textAlign: 'center',
})

export const categorySection = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
})

export const sectionHead = style({
  alignItems: 'baseline',
  display: 'flex',
  gap: 8,
})

export const sectionCount = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 999,
  color: theme.content.secondary,
  display: 'inline-flex',
  fontSize: 11,
  fontWeight: 600,
  height: 18,
  justifyContent: 'center',
  minWidth: 22,
  padding: '0 6px',
})

export const searchBar = style({
  maxWidth: 360,
})
