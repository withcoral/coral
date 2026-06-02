import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const header = style({
  alignItems: 'flex-start',
  display: 'flex',
  gap: 12,
})

export const headerLogo = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  borderRadius: '50%',
  display: 'flex',
  flexShrink: 0,
  height: 40,
  justifyContent: 'center',
  overflow: 'hidden',
  width: 40,
})

export const headerLogoImg = style({
  height: '100%',
  objectFit: 'cover',
  width: '100%',
})

export const headerText = style({
  display: 'flex',
  flexDirection: 'column',
  flexGrow: 1,
  gap: 4,
  minWidth: 0,
})

export const headerTitleRow = style({
  alignItems: 'center',
  display: 'flex',
  gap: 10,
  marginInlineEnd: 24,
})

export const headerTitle = style({
  textTransform: 'capitalize',
})

export const headerPill = style({
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

// Kept so the public API of <Field> is unchanged even though all fields are
// now full-width — selecting this className is a no-op against the flex
// column layout above.
export const fieldItemFull = style({})

export const fieldLabel = style({
  color: theme.content.primary,
  fontWeight: 500,
})

export const methodTabs = style({
  background: theme.surface.onMainContent,
  borderRadius: 8,
  display: 'inline-flex',
  gap: 4,
  marginBlockEnd: 4,
  padding: 4,
  width: 'fit-content',
})

export const methodTab = style({
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

export const oauthFields = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
})

export const oauthBox = style({
  alignItems: 'flex-start',
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  gap: 10,
  padding: 12,
})

export const oauthCode = style({
  color: theme.content.primary,
  fontFamily: 'monospace',
  fontWeight: 700,
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
