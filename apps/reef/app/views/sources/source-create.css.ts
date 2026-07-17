import { style } from '@vanilla-extract/css'

import { fontFamily } from '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

export const header = style({
  alignItems: 'center',
  gap: 10,
  marginBlockEnd: 14,
  paddingBlockEnd: 0,
})

export const dialogContent = style({
  display: 'flex',
  flexDirection: 'column',
})

export const fieldGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 14,
})

export const fieldItem = style({
  gap: 6,
})

export const authPanelStack = style({
  display: 'grid',
})

export const authPanel = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 14,
  gridArea: '1 / 1',
  minWidth: 0,
})

export const authPanelHidden = style({
  pointerEvents: 'none',
  visibility: 'hidden',
})

export const summaryBox = style({
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  padding: 12,
})

export const summaryRow = style({
  display: 'flex',
  gap: 8,
})

export const summaryKey = style({
  color: theme.content.tertiary,
  flex: '0 0 96px',
})

export const summaryValue = style({
  color: theme.content.primary,
  fontFamily: fontFamily.dmMono,
  fontSize: 12,
  minWidth: 0,
  overflowWrap: 'anywhere',
})

export const importError = style({
  marginBlockStart: 14,
})
