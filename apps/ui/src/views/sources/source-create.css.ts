import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme'
import { fontFamily } from '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

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
  maxWidth: 1280,
  width: '100%',
})

export const header = style({
  alignItems: 'flex-start',
  display: 'flex',
  gap: 16,
  justifyContent: 'space-between',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      flexDirection: 'column',
    },
  },
})

export const headerText = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})

export const headerActions = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  justifyContent: 'flex-end',
})

export const layout = style({
  alignItems: 'flex-start',
  display: 'grid',
  gap: 24,
  gridTemplateColumns: 'minmax(0, 1fr) minmax(360px, 440px)',
  '@media': {
    [`screen and (max-width: ${breakpoints.sidebarCollapse})`]: {
      gridTemplateColumns: '1fr',
    },
  },
})

export const formColumn = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  minWidth: 0,
  width: '100%',
})

export const previewColumn = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  minWidth: 0,
  position: 'sticky',
  top: 0,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.sidebarCollapse})`]: {
      position: 'static',
    },
  },
})

export const panel = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  padding: 16,
})

export const panelHead = style({
  alignItems: 'center',
  display: 'flex',
  gap: 12,
  justifyContent: 'space-between',
})

export const fieldGrid = style({
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gridTemplateColumns: '1fr',
    },
  },
})

export const field = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  minWidth: 0,
})

export const select = style({
  background: theme.surface.mainContent,
  border: `1px solid ${theme.input.stroke.default}`,
  borderRadius: 8,
  color: theme.content.primary,
  fontFamily: 'inherit',
  fontSize: 13,
  height: 34,
  minWidth: 0,
  outline: 'none',
  paddingBlock: 0,
  paddingInline: 10,
  width: '100%',
  ':focus': {
    borderColor: theme.input.stroke.focus,
  },
})

export const textarea = style({
  background: theme.surface.mainContent,
  border: `1px solid ${theme.input.stroke.default}`,
  borderRadius: 8,
  color: theme.content.primary,
  fontFamily: 'inherit',
  fontSize: 13,
  lineHeight: '20px',
  minHeight: 96,
  outline: 'none',
  padding: 10,
  resize: 'vertical',
  width: '100%',
  ':focus': {
    borderColor: theme.input.stroke.focus,
  },
})

export const manifestTextarea = style([
  textarea,
  {
    fontFamily: fontFamily.dmMono,
    fontSize: 12,
    lineHeight: '18px',
    minHeight: 520,
    whiteSpace: 'pre',
  },
])

export const stack = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
})

export const stackSmall = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
})

export const itemPanel = style({
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 14,
  padding: 14,
})

export const itemHeader = style({
  alignItems: 'flex-start',
  display: 'flex',
  gap: 12,
  justifyContent: 'space-between',
})

export const checkRow = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  minHeight: 34,
})

export const segmented = style({
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'inline-flex',
  padding: 3,
  width: 'fit-content',
})

export const segment = style({
  background: 'transparent',
  border: 'none',
  borderRadius: 6,
  color: theme.content.secondary,
  cursor: 'pointer',
  fontSize: 12,
  fontWeight: 600,
  minHeight: 26,
  paddingBlock: 4,
  paddingInline: 10,
  selectors: {
    '&[data-active="true"]': {
      background: theme.surface.card,
      color: theme.content.primary,
    },
  },
})

export const subsectionHead = style({
  alignItems: 'center',
  display: 'flex',
  gap: 12,
  justifyContent: 'space-between',
})

export const headerRow = style({
  alignItems: 'center',
  display: 'grid',
  gap: 8,
  gridTemplateColumns: 'minmax(120px, 1fr) 128px minmax(160px, 1.2fr) auto',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      alignItems: 'stretch',
      gridTemplateColumns: '1fr',
    },
  },
})

export const issueBox = style({
  alignItems: 'flex-start',
  background: theme.pill.amber.background,
  border: `1px solid ${theme.pill.amber.stroke}`,
  borderRadius: 8,
  color: theme.pill.amber.color,
  display: 'flex',
  gap: 10,
  padding: 12,
})

export const issueList = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  margin: 0,
  paddingInlineStart: 18,
})

export const resultHeader = style({
  alignItems: 'flex-start',
  display: 'flex',
  gap: 12,
  justifyContent: 'space-between',
})

export const resultSection = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 10,
})

export const resultGrid = style({
  display: 'grid',
  gap: 8,
  gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
})

export const resultCard = style({
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
  padding: 10,
})

export const queryResult = style({
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  minWidth: 0,
  padding: 10,
})
