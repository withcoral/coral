// PROTOTYPE — throwaway styles for handoff.prototype.tsx. Not shipped.
import { style, styleVariants } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

const code = theme.typography.code.fontFamily

export const switcher = style({
  position: 'fixed',
  top: 12,
  right: 16,
  zIndex: 1000,
  display: 'flex',
  alignItems: 'center',
  gap: 12,
  padding: '6px 10px',
  borderRadius: 999,
  background: '#111',
  color: '#fff',
  boxShadow: '0 6px 24px rgba(0,0,0,0.35)',
  font: '13px/1.4 system-ui, sans-serif',
})
export const switcherButton = style({
  all: 'unset',
  cursor: 'pointer',
  padding: '0 6px',
  borderRadius: 4,
  ':hover': { background: '#333' },
})

export const sheet = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 20,
  maxWidth: 760,
  width: '100%',
  margin: '0 auto',
  paddingBottom: 96,
})
export const sheetHeader = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
})
export const runId = style({
  fontFamily: code,
  fontSize: 26,
  lineHeight: '32px',
  letterSpacing: '-0.01em',
  overflowWrap: 'anywhere',
})
export const runIdSmall = style({
  fontFamily: code,
  fontSize: 15,
  lineHeight: '20px',
})
export const row = style({
  display: 'flex',
  alignItems: 'center',
  gap: 12,
  flexWrap: 'wrap',
  minWidth: 0,
})
export const spread = style({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 12,
  flexWrap: 'wrap',
  minWidth: 0,
})
export const stack = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  minWidth: 0,
})
export const stackTight = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  minWidth: 0,
})

const card = {
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  padding: 16,
  borderRadius: 10,
  background: theme.surface.card,
} as const
export const requestCard = styleVariants({
  plain: { ...card, border: `1px solid ${theme.stroke.secondary}` },
  linked: { ...card, border: `2px solid ${theme.stroke.focused}` },
  settled: {
    ...card,
    border: `1px solid ${theme.stroke.secondary}`,
    background: theme.surface.onMainContentSubtle,
    opacity: 0.8,
  },
})
export const cardActions = style({
  display: 'flex',
  justifyContent: 'flex-end',
  gap: 8,
  flexWrap: 'wrap',
})
export const stickyBar = style({
  position: 'sticky',
  bottom: 0,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 12,
  flexWrap: 'wrap',
  padding: '12px 16px',
  borderTop: `1px solid ${theme.stroke.secondary}`,
  background: theme.surface.mainContent,
})
export const closing = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  padding: 20,
  borderRadius: 10,
  border: `1px solid ${theme.stroke.secondary}`,
  background: theme.surface.card,
  textAlign: 'center',
  alignItems: 'center',
})

export const split = style({
  display: 'grid',
  gridTemplateColumns: 'minmax(0, 5fr) minmax(0, 6fr)',
  gap: 24,
  minHeight: 0,
  flex: 1,
  '@media': { '(max-width: 900px)': { gridTemplateColumns: 'minmax(0, 1fr)' } },
})
export const panelFrame = style({
  display: 'flex',
  flexDirection: 'column',
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 10,
  background: theme.surface.card,
  minWidth: 0,
  overflow: 'hidden',
  alignSelf: 'start',
})
export const panelBody = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  padding: 16,
  minWidth: 0,
})
export const runStrip = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  padding: '12px 16px',
  borderBottom: `1px solid ${theme.stroke.secondary}`,
  background: theme.surface.onMainContentSubtle,
})
export const panelFooterBar = style({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'flex-end',
  gap: 8,
  flexWrap: 'wrap',
  padding: '12px 16px',
  borderTop: `1px solid ${theme.stroke.secondary}`,
})
export const historyStub = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  padding: 12,
  border: `1px dashed ${theme.stroke.secondary}`,
  borderRadius: 8,
})

export const sourceLayout = style({
  display: 'grid',
  gridTemplateColumns: 'minmax(0, 3fr) minmax(280px, 2fr)',
  gap: 20,
  alignItems: 'start',
  '@media': { '(max-width: 900px)': { gridTemplateColumns: 'minmax(0, 1fr)' } },
})
export const sourceFrame = style({
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 10,
  overflow: 'hidden',
  minWidth: 0,
})
export const stopRail = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 10,
  position: 'sticky',
  top: 12,
})
const stopBase = {
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  padding: 12,
  borderRadius: 10,
  background: theme.surface.card,
  boxSizing: 'border-box',
  width: '100%',
} as const
export const stop = styleVariants({
  plain: { ...stopBase, border: `1px solid ${theme.stroke.secondary}` },
  current: { ...stopBase, border: `2px solid ${theme.stroke.focused}` },
})
export const stopPick = style({
  all: 'unset',
  cursor: 'pointer',
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  ':focus-visible': {
    outline: `2px solid ${theme.stroke.focused}`,
    outlineOffset: 2,
  },
})
export const lineTag = style({
  fontFamily: code,
  fontSize: 12,
  padding: '1px 6px',
  borderRadius: 4,
  background: theme.surface.onMainContent,
})
