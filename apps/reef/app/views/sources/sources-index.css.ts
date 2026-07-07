import { keyframes, style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
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
  minHeight: 0,
  overflow: 'hidden',
})

export const header = style({
  flexShrink: 0,
})

export const headerInner = style({
  alignItems: 'flex-start',
  boxSizing: 'border-box',
  display: 'flex',
  gap: 24,
  justifyContent: 'space-between',
  marginInline: 'auto',
  maxWidth: 960,
  paddingBlock: 24,
  paddingInline: 32,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      flexDirection: 'column',
      gap: 16,
      paddingBlock: 20,
      paddingInline: 16,
    },
  },
})

export const headerText = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: 4,
  maxWidth: 680,
  minWidth: 0,
})

export const searchBar = style({
  flex: '0 0 360px',
  maxWidth: 360,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      flexBasis: 'auto',
      maxWidth: '100%',
    },
  },
})

export const statusPanel = style({
  boxSizing: 'border-box',
  flexShrink: 0,
  marginInline: 'auto',
  maxWidth: 960,
  paddingBlock: 16,
  paddingInline: 32,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      paddingInline: 16,
    },
  },
})

export const resultsScroll = style({
  flex: 1,
  minHeight: 0,
})

export const resultsContent = style({
  boxSizing: 'border-box',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 32,
  marginInline: 'auto',
  maxWidth: 960,
  minHeight: '100%',
  paddingBlock: 32,
  paddingInline: 32,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      paddingBlock: 24,
      paddingInline: 16,
    },
  },
})

export const loadingState = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  justifyContent: 'center',
  paddingBlock: 48,
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
