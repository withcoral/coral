import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  background: theme.surface.mainContent,
  boxSizing: 'border-box',
  minHeight: '100dvh',
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

export const content = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 30,
  marginInline: 'auto',
  maxWidth: 960,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gap: 22,
    },
  },
})

export const header = style({
  width: '100%',
})

export const headerText = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: 6,
  maxWidth: 640,
  minWidth: 0,
})

export const titleRow = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  minWidth: 0,
})

export const stepPill = style({
  flexShrink: 0,
})

export const body = style({
  alignItems: 'start',
  display: 'grid',
  gap: 28,
  gridTemplateColumns: 'minmax(240px, 280px) minmax(0, 720px)',
  justifyContent: 'center',
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gridTemplateColumns: '1fr',
    },
  },
})

export const explainer = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 22,
  paddingBlockStart: 16,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      paddingBlockStart: 0,
    },
  },
})

export const explainerText = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
})

export const inlineLink = style({
  color: theme.content.link,
  textDecoration: 'underline',
  textUnderlineOffset: 2,
  selectors: {
    '&:hover': {
      color: theme.content.linkHover,
    },
    '&:focus-visible': {
      borderRadius: 2,
      outline: `1px solid ${theme.stroke.focused}`,
      outlineOffset: 2,
    },
  },
})

export const mainFrame = style({
  alignSelf: 'center',
  background: theme.surface.mainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  boxSizing: 'border-box',
  height: 'min(620px, calc(100dvh - 204px))',
  maxWidth: 720,
  minHeight: 480,
  overflow: 'hidden',
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      height: 'min(600px, calc(100dvh - 236px))',
      minHeight: 460,
    },
  },
})
