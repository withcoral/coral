import { recipe } from '@vanilla-extract/recipes'

import { animation, theme } from '@/wax/theme/theme.css'

export const iconContainer = recipe({
  base: {
    alignItems: 'center',
    display: 'inline-flex',
    flexShrink: 0,
    justifyContent: 'center',
    strokeLinecap: 'round',
    strokeWidth: '1.25px',
    transition: animation.colorTransition,
  },
  defaultVariants: {
    color: 'primary',
    size: '20',
  },
  variants: {
    color: {
      disabled: { color: theme.content.disabled },
      error: { color: theme.content.error },
      info: { color: theme.content.info },
      inherit: {},
      orange: { color: theme.pill.orange.color },
      placeholder: { color: theme.content.placeholder },
      primary: { color: theme.content.primary },
      secondary: { color: theme.content.secondary },
      success: { color: theme.content.success },
      tertiary: { color: theme.content.tertiary },
      warning: { color: theme.content.warning },
    },
    size: {
      '14': {},
      '16': {},
      '18': {},
      '20': {},
      '24': {},
      '30': {},
    },
  },
})
