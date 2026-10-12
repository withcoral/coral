import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const frame = style({
  maxWidth: 1120,
  marginInline: 'auto',
  paddingInline: 24,
  '@media': { '(max-width: 640px)': { paddingInline: 16 } },
})
export const header = style({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  flexWrap: 'wrap',
  gap: 12,
  paddingBlock: 20,
  borderBottom: `1px solid ${theme.stroke.secondary}`,
})
export const main = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 32,
  paddingBlock: 32,
  minWidth: 0,
})
export const stack = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })
export const intro = style({ display: 'flex', flexDirection: 'column', gap: 8, minWidth: 0 })
export const row = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  gap: '8px 16px',
})
export const sectionHeading = style({
  display: 'flex',
  alignItems: 'baseline',
  flexWrap: 'wrap',
  justifyContent: 'space-between',
  gap: 12,
})
export const heading = style({ overflowWrap: 'anywhere' })
export const footer = style({ paddingBlock: 24, borderTop: `1px solid ${theme.stroke.secondary}` })
export const skip = style({ position: 'absolute', top: -100, left: 16, ':focus': { top: 8 } })
export const empty = style({ padding: 24 })
export const sessionLink = style({
  display: 'flex',
  flexDirection: 'row',
  alignItems: 'center',
  gap: 16,
  minWidth: 0,
})
export const sessionLabel = style({ flex: 1, minWidth: 0, overflowWrap: 'anywhere' })
export const history = style({ display: 'flex', flexDirection: 'column', gap: 12 })
export const historyItem = style({
  display: 'flex',
  flexDirection: 'row',
  flexWrap: 'wrap',
  justifyContent: 'space-between',
  gap: 16,
})
export const media = style({
  display: 'block',
  width: '100%',
  aspectRatio: '16 / 10',
  maxHeight: 640,
  background: theme.surface.main,
  borderRadius: 8,
})
export const steps = style({
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(min(100%, 240px), 1fr))',
  gap: 16,
})
export const screenshot = style({
  display: 'block',
  width: '100%',
  aspectRatio: '16 / 10',
  objectFit: 'contain',
  background: theme.surface.main,
})
export const caption = style({ display: 'flex', gap: 12, alignItems: 'baseline' })
export const code = style({
  whiteSpace: 'pre-wrap',
  overflowWrap: 'anywhere',
  maxHeight: 600,
  overflow: 'auto',
  tabSize: 2,
})
export const failure = style({ maxHeight: 280 })
export const summary = style({
  cursor: 'pointer',
  ':focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: 4 },
})
export const detailsContent = style({ paddingTop: 16 })
export const duration = style({ fontVariantNumeric: 'tabular-nums', whiteSpace: 'nowrap' })
export const scenarioDepth = '--reel-scenario-depth'
export const scenarioIndent = style({
  minWidth: 0,
  paddingInlineStart: `calc(var(${scenarioDepth}) * 20px)`,
})
