import { style } from '@vanilla-extract/css'
import { recipe, RecipeVariants } from '@vanilla-extract/recipes'

import { utils } from '@/styles/utils'
import { theme } from '@/wax/theme/theme.css'

export const typography = recipe({
  base: {
    color: theme.content.primary,
  },
  defaultVariants: {
    variant: 'body',
  },
  variants: {
    color: {
      code: {
        color: theme.content.code.inlineColor,
      },
      disabled: {
        color: theme.content.disabled,
      },
      error: {
        color: theme.content.error,
      },
      placeholder: {
        color: theme.content.placeholder,
      },
      primary: {
        color: theme.content.primary,
      },
      secondary: {
        color: theme.content.secondary,
      },
      tertiary: {
        color: theme.content.tertiary,
      },
    },
    variant: {
      body: { ...theme.typography.body },
      bodyLarge: { ...theme.typography.bodyLarge },
      bodyLargeStrong: { ...theme.typography.bodyLargeStrong },
      bodySmall: { ...theme.typography.bodySmall },
      bodySmallStrong: { ...theme.typography.bodySmallStrong },
      bodyStrong: { ...theme.typography.bodyStrong },
      buttonStrong: { ...theme.typography.buttonStrong },
      code: { ...theme.typography.code },
      codeInline: { ...theme.typography.codeInline },
      codeInlineStrong: { ...theme.typography.codeInlineStrong },
      codeLarge: { ...theme.typography.codeLarge },
      codeSmallInline: { ...theme.typography.codeSmallInline },
      codeSmallInlineStrong: { ...theme.typography.codeSmallInlineStrong },
      headingLarge: { ...theme.typography.headingLarge },
      headingMedium: { ...theme.typography.headingMedium },
      headingSmall: { ...theme.typography.headingSmall },
      headingXLarge: { ...theme.typography.headingXLarge },
      headingXSmall: { ...theme.typography.headingXSmall },
    },
  },
})

export const truncate = style({ ...utils.boxClamp(1) })

export type ColorVariants = NonNullable<RecipeVariants<typeof typography>>['color']
