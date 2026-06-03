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

export const cardGrid = style({
  display: 'grid',
  gap: 16,
  gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
})

export const card = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 12,
  color: 'inherit',
  cursor: 'pointer',
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  padding: 16,
  textAlign: 'left',
  textDecoration: 'none',
  transition: 'background 80ms ease, border-color 80ms ease',
  ':hover': {
    background: theme.surface.onMainContentHover,
  },
})

export const cardHeader = style({
  alignItems: 'center',
  display: 'flex',
  gap: 10,
})

export const cardLogo = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  borderRadius: '50%',
  display: 'flex',
  flexShrink: 0,
  height: 28,
  justifyContent: 'center',
  overflow: 'hidden',
  width: 28,
})

export const cardLogoImg = style({
  height: '100%',
  objectFit: 'cover',
  width: '100%',
})

export const cardTitle = style({
  flexGrow: 1,
  textTransform: 'capitalize',
})

export const cardDescription = style({
  display: '-webkit-box',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  WebkitBoxOrient: 'vertical' as const,
  WebkitLineClamp: 2,
})

export const cardFooter = style({
  alignItems: 'center',
  color: theme.content.tertiary,
  display: 'flex',
  gap: 8,
  justifyContent: 'flex-end',
  marginBlockStart: 'auto',
})

export const connectedPill = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 999,
  color: theme.content.secondary,
  display: 'inline-flex',
  flexShrink: 0,
  fontSize: 11,
  fontWeight: 600,
  padding: '2px 8px',
})

export const originPill = style({
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
