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

export const fieldGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 14,
})

export const fieldItem = style({
  gap: 6,
})

export const hiddenFileInput = style({
  display: 'none',
})

export const orDivider = style({
  alignItems: 'center',
  display: 'flex',
  gap: 12,
  selectors: {
    '&::before, &::after': {
      background: theme.stroke.primary,
      blockSize: 1,
      content: '""',
      flex: 1,
    },
  },
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
