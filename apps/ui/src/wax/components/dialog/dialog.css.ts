import { style, styleVariants } from '@vanilla-extract/css'

import { utils } from '@/styles/utils'
import { staticUtilities, theme, zIndex } from '@/wax/theme/theme.css'

export const backdrop = style({
  backgroundColor: staticUtilities.black,
  inset: 0,
  opacity: 0.2,
  position: 'fixed',
  selectors: {
    '&[data-starting-style], &[data-ending-style]': {
      opacity: 0,
    },
  },
  transition: 'opacity 150ms cubic-bezier(0.45, 1.005, 0, 1.005)',
  zIndex: zIndex.modalBackdrop,
})

export const popup = style({
  backgroundColor: theme.surface.floating,
  border: `1px solid ${theme.stroke.floating}`,
  borderRadius: '10px',
  boxShadow: theme.elevation.e4,
  display: 'flex',
  flexDirection: 'column',
  gap: '12px',
  insetBlockStart: '50%',
  insetInlineStart: '50%',
  maxHeight: 'calc(100vh - 64px)',
  maxWidth: 'calc(100vw - 48px)',
  overflow: 'auto',
  padding: '24px',
  position: 'fixed',
  selectors: {
    '&[data-nested-dialog-open]::after': {
      backgroundColor: utils.opacify(staticUtilities.black, 0.05),
      borderRadius: 'inherit',
      content: '',
      inset: 0,
      position: 'absolute',
    },
    '&[data-starting-style], &[data-ending-style]': {
      opacity: 0,
      transform: 'translate(-50%, -50%) scale(0.9)',
    },
  },
  transform: 'translate(-50%, -50%) scale(calc(1 - 0.1 * var(--nested-dialogs)))',
  transition: 'all 150ms',
  translate: '0 calc(0px + 16px * var(--nested-dialogs))',
  zIndex: zIndex.modal,
})

export const popupSize = styleVariants({
  l: { width: '600px' },
  m: { width: '400px' },
  xl: { width: '700px' },
})

export const title = style({
  color: theme.content.primary,
  ...theme.typography.headingMedium,
  ...utils.boxClamp(1),
  marginInlineEnd: '24px',
})

export const description = style({
  color: theme.content.secondary,
  ...theme.typography.body,
})

export const close = style({
  insetBlockStart: '16px',
  insetInlineEnd: '16px',
  position: 'absolute',
})

export const actions = style({
  display: 'flex',
  gap: '10px',
  justifyContent: 'flex-end',
  marginBlockStart: '12px',
})
