import { style } from '@vanilla-extract/css'
import { recipe } from '@vanilla-extract/recipes'

import { theme } from '@/wax/theme/theme.css'

export const banner = recipe({
  base: {
    alignItems: 'flex-start',
    border: '1px solid',
    borderRadius: '10px',
    display: 'flex',
    gap: '12px',
    paddingBlock: '10px',
    paddingInline: '12px',
  },
  defaultVariants: {
    variant: 'info',
  },
  variants: {
    variant: {
      error: {
        background: theme.pill.red.background,
        borderColor: theme.pill.red.stroke,
        color: theme.pill.red.color,
      },
      info: {
        background: theme.pill.blue.background,
        borderColor: theme.pill.blue.stroke,
        color: theme.pill.blue.color,
      },
      success: {
        background: theme.pill.green.background,
        borderColor: theme.pill.green.stroke,
        color: theme.pill.green.color,
      },
      warning: {
        background: theme.pill.amber.background,
        borderColor: theme.pill.amber.stroke,
        color: theme.pill.amber.color,
      },
    },
  },
})

export const icon = style({
  marginBlockStart: '1px',
})

export const content = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: '2px',
  minWidth: 0,
  textAlign: 'start',
})

export const title = style({ ...theme.typography.bodySmallStrong })

export const message = style({ ...theme.typography.bodySmall })

export const action = style({
  alignSelf: 'center',
  flexShrink: 0,
})
