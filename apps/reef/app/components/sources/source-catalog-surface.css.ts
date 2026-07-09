import { keyframes, style, styleVariants } from '@vanilla-extract/css'

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

export const rootVariant = styleVariants({
  compact: {
    background: theme.surface.mainContent,
  },
  full: {},
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
  width: '100%',
})

export const headerInnerVariant = styleVariants({
  compact: {
    gap: 16,
    maxWidth: 'none',
    paddingBlock: 16,
    paddingInline: 16,
    '@media': {
      [`screen and (max-width: ${breakpoints.mobile})`]: {
        flexDirection: 'column',
        gap: 12,
        paddingBlock: 14,
        paddingInline: 14,
      },
    },
  },
  full: {
    maxWidth: 960,
    paddingBlock: 24,
    paddingInline: 32,
    '@media': {
      [`screen and (max-width: ${breakpoints.mobile})`]: {
        flexDirection: 'column',
        gap: 16,
        paddingBlock: 20,
        paddingInline: 16,
      },
    },
  },
})

export const headerText = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})

export const headerTextVariant = styleVariants({
  compact: {
    maxWidth: 420,
  },
  full: {
    maxWidth: 680,
  },
})

export const searchBar = style({
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      flexBasis: 'auto',
      maxWidth: '100%',
    },
  },
})

export const searchBarVariant = styleVariants({
  compact: {
    flex: '0 0 280px',
    maxWidth: 280,
  },
  full: {
    flex: '0 0 360px',
    maxWidth: 360,
  },
})

export const statusPanel = style({
  boxSizing: 'border-box',
  flexShrink: 0,
  marginInline: 'auto',
  width: '100%',
})

export const statusPanelVariant = styleVariants({
  compact: {
    maxWidth: 'none',
    paddingBlock: 12,
    paddingInline: 16,
    '@media': {
      [`screen and (max-width: ${breakpoints.mobile})`]: {
        paddingInline: 14,
      },
    },
  },
  full: {
    maxWidth: 960,
    paddingBlock: 16,
    paddingInline: 32,
    '@media': {
      [`screen and (max-width: ${breakpoints.mobile})`]: {
        paddingInline: 16,
      },
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
  marginInline: 'auto',
  minHeight: '100%',
  width: '100%',
})

export const resultsContentVariant = styleVariants({
  compact: {
    gap: 22,
    maxWidth: 'none',
    paddingBlock: 18,
    paddingInline: 16,
    '@media': {
      [`screen and (max-width: ${breakpoints.mobile})`]: {
        paddingBlock: 16,
        paddingInline: 14,
      },
    },
  },
  full: {
    gap: 32,
    maxWidth: 960,
    paddingBlock: 32,
    paddingInline: 32,
    '@media': {
      [`screen and (max-width: ${breakpoints.mobile})`]: {
        paddingBlock: 24,
        paddingInline: 16,
      },
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

export const categorySectionVariant = styleVariants({
  compact: {
    gap: 12,
  },
  full: {},
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
