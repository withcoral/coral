import { createVar } from '@vanilla-extract/css'
import { recipe, RecipeVariants } from '@vanilla-extract/recipes'

import { animation, staticUtilities, theme } from '@/wax/theme/theme.css'

import { button, disabledClass } from './button.css'

export const textColor = createVar()

const baseStyles = {
  color: textColor,
  display: 'inline-block',
  fontWeight: 500,
  overflow: 'hidden',
  selectors: {
    [`.${disabledClass} &`]: {
      color: theme.content.disabled,
    },
  },
  textOverflow: 'ellipsis',
  transition: animation.colorTransition,
  verticalAlign: 'bottom',
  whiteSpace: 'nowrap',
} as const

export const text = recipe({
  base: baseStyles,

  compoundVariants: [
    {
      style: theme.typography.bodySmall,
      variants: { buttonVariant: 'link', size: '22' },
    },
    {
      style: theme.typography.body,
      variants: { buttonVariant: 'link', size: '32' },
    },
    {
      style: theme.typography.bodyLarge,
      variants: { buttonVariant: 'link', size: '36' },
    },
    {
      style: theme.typography.bodySmall,
      variants: { buttonVariant: 'linkSubtle', size: '22' },
    },
    {
      style: theme.typography.body,
      variants: { buttonVariant: 'linkSubtle', size: '32' },
    },
    {
      style: theme.typography.bodyLarge,
      variants: { buttonVariant: 'linkSubtle', size: '36' },
    },
  ],

  defaultVariants: {
    buttonVariant: 'primary',
    size: '32',
  },

  variants: {
    buttonVariant: {
      bare: {
        selectors: {
          [`.${button.classNames.base}:hover:not(.${disabledClass}) &`]: {
            color: theme.content.primary,
          },
        },
        vars: {
          [textColor]: theme.content.secondary,
        },
      },
      destructive: {
        vars: {
          [textColor]: staticUtilities.white,
        },
      },
      link: {
        selectors: {
          [`.${button.classNames.base}:hover:not(.${disabledClass}) &`]: {
            color: theme.content.accentContent.primary,
          },
        },
        vars: {
          [textColor]: theme.content.primary,
        },
      },
      linkSubtle: {
        selectors: {
          [`.${button.classNames.base}:hover:not(.${disabledClass}) &`]: {
            color: theme.content.primary,
          },
        },
        vars: {
          [textColor]: theme.content.tertiary,
        },
      },
      primary: {
        vars: {
          [textColor]: theme.content.accentContent.primaryReverse,
        },
      },
      secondary: {
        selectors: {
          [`.${button.classNames.base}:hover:not(.${disabledClass}) &`]: {
            color: theme.content.primary,
          },
        },
        vars: {
          [textColor]: theme.content.secondary,
        },
      },
    },

    size: {
      '22': {
        ...theme.typography.bodySmall,
      },
      '32': {
        ...theme.typography.buttonStrong,
      },
      '36': {
        ...theme.typography.buttonStrong,
      },
    },
  },
})

export type TextVariants = RecipeVariants<typeof text>
