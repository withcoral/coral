import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const dialogContent = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '16px',
})

export const header = style({
  alignItems: 'center',
  gap: 10,
  marginBlockEnd: 14,
  paddingBlockEnd: 0,
})

export const sourceChoices = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
})

export const hiddenFileInput = style({
  display: 'none',
})

export const manifestDropZone = style({
  alignItems: 'center',
  border: `1px dashed ${theme.stroke.primary}`,
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  justifyContent: 'center',
  minBlockSize: 168,
  paddingBlock: 24,
  paddingInline: 16,
  selectors: {
    '&[data-dropping]': {
      background: theme.surface.onMainContentHover,
      borderColor: theme.stroke.focused,
    },
  },
  textAlign: 'center',
})

export const manifestDropHint = style({
  alignItems: 'center',
  display: 'inline-flex',
  gap: '4px',
})
