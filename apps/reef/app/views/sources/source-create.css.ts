import { style } from '@vanilla-extract/css'

import { fontFamily } from '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

export const header = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  marginBlockEnd: 6,
})

export const dialogContent = style({
  display: 'flex',
  flexDirection: 'column',
})

export const stepLabel = style({
  color: theme.content.tertiary,
})

export const fieldGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 14,
})

export const fieldItem = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
})

export const fieldLabel = style({
  color: theme.content.primary,
  fontWeight: 500,
})

export const choiceTabs = style({
  background: theme.surface.onMainContent,
  borderRadius: 8,
  display: 'inline-flex',
  gap: 4,
  padding: 4,
  width: 'fit-content',
})

export const choiceTab = style({
  background: 'transparent',
  border: 'none',
  borderRadius: 6,
  color: theme.content.secondary,
  cursor: 'pointer',
  fontSize: 12,
  fontWeight: 500,
  padding: '4px 10px',
  transition: 'background 80ms ease, color 80ms ease',
  ':disabled': { cursor: 'not-allowed', opacity: 0.6 },
  ':hover': { background: theme.surface.onMainContentHover, color: theme.content.primary },
  selectors: {
    '&[data-active="true"]': {
      background: theme.surface.card,
      color: theme.content.primary,
    },
  },
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

export const alertBox = style({
  alignItems: 'center',
  borderRadius: 6,
  display: 'flex',
  fontSize: 12,
  gap: 8,
  lineHeight: '16px',
  paddingBlock: 8,
  paddingInline: 12,
})

export const alertError = style({
  background: theme.pill.red.background,
  border: `1px solid ${theme.pill.red.stroke}`,
  color: theme.pill.red.color,
})
