import { recipe, RecipeVariants } from '@vanilla-extract/recipes'

import { staticUtilities, theme } from '@/wax/theme/theme.css'

import { button, disabledClass } from './button.css'

export const iconContainer = recipe({
  base: {
    selectors: {
      [`.${disabledClass} &`]: {
        color: theme.content.disabled,
      },
    },
  },

  defaultVariants: {
    buttonVariant: 'primary',
    size: '32',
  },

  variants: {
    buttonVariant: {
      bare: {
        color: theme.content.tertiary,
        selectors: {
          [`.${button.classNames.base}:hover:not(.${disabledClass}) &`]: {
            color: theme.content.primary,
          },
        },
      },
      destructive: {
        color: staticUtilities.white,
      },
      link: {
        color: theme.content.primary,
        selectors: {
          [`.${button.classNames.base}:hover:not(.${disabledClass}) &`]: {
            color: theme.content.accentContent.primary,
          },
        },
      },
      linkSubtle: {
        color: theme.content.tertiary,
        selectors: {
          [`.${button.classNames.base}:hover:not(.${disabledClass}) &`]: {
            color: theme.content.primary,
          },
        },
      },
      primary: {
        color: theme.content.accentContent.primaryReverse,
      },
      secondary: {
        color: theme.content.secondary,
        selectors: {
          [`.${button.classNames.base}:hover:not(.${disabledClass}) &`]: {
            color: theme.content.primary,
          },
        },
      },
    },

    size: {
      '22': {},
      '32': {},
      '36': {},
    },
  },
})

export type IconContainerVariants = RecipeVariants<typeof iconContainer>
