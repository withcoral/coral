import { style } from '@vanilla-extract/css'
import { recipe, RecipeVariants } from '@vanilla-extract/recipes'

import { animation, theme } from '@/wax/theme/theme.css'

export const disabledClass = style({})
export const activeClass = style({})

const baseStyles = {
  alignItems: 'center',
  background: 'transparent',
  border: 'none',
  borderRadius: '8px',
  cursor: 'pointer',
  display: 'flex',
  flexGrow: 0,
  flexShrink: 0,
  gap: '10px',
  justifyContent: 'flex-start',
  margin: 0,
  outline: 'none',
  paddingBlock: '6px',
  paddingInline: '8px',
  position: 'relative',
  textDecoration: 'none',
  transition: animation.colorTransition,
  width: '100%',
} as const

export const sidebarButton = recipe({
  base: baseStyles,
  defaultVariants: {
    disabled: false,
    isActive: false,
    isMinimized: false,
    variant: 'default',
  },
  variants: {
    disabled: { false: {}, true: { cursor: 'default' } },
    isActive: { false: {}, true: {} },
    isMinimized: {
      false: {},
      true: { gap: 0, justifyContent: 'center', paddingInline: 0, width: '34px' },
    },
    variant: {
      accent: {
        selectors: {
          '&:focus-visible': { outline: `1px solid ${theme.button.primary.focus}` },
          [`&.${activeClass}`]: { background: theme.sidebar.buttonAccent.selected },
          [`&:hover:not(.${disabledClass})`]: { background: theme.sidebar.buttonAccent.hover },
        },
      },
      default: {
        selectors: {
          '&:focus-visible': { outline: `1px solid ${theme.button.primary.focus}` },
          [`&.${activeClass}`]: { background: theme.sidebar.button.selected },
          [`&:hover:not(.${disabledClass})`]: { background: theme.sidebar.button.hover },
        },
      },
    },
  },
})

export const iconStyles = recipe({
  base: {
    flexShrink: 0,
    selectors: { [`.${disabledClass} &`]: { color: theme.content.disabled } },
    transition: animation.colorTransition,
  },
  defaultVariants: { variant: 'default' },
  variants: {
    variant: {
      accent: {
        color: theme.content.accentContent.secondary,
        selectors: { [`.${activeClass} &`]: { color: theme.content.accentContent.primary } },
      },
      default: {
        color: theme.content.tertiary,
        selectors: { [`.${activeClass} &`]: { color: theme.content.primary } },
      },
    },
  },
})

export const textStyles = recipe({
  base: {
    ...theme.typography.buttonStrong,
    overflow: 'hidden',
    selectors: { [`.${disabledClass} &`]: { color: theme.content.disabled } },
    textOverflow: 'ellipsis',
    transition: animation.colorTransition,
    whiteSpace: 'nowrap',
  },
  defaultVariants: { variant: 'default' },
  variants: {
    variant: {
      accent: {
        color: theme.content.accentContent.secondary,
        selectors: { [`.${activeClass} &`]: { color: theme.content.accentContent.primary } },
      },
      default: {
        color: theme.content.secondary,
        selectors: { [`.${activeClass} &`]: { color: theme.content.primary } },
      },
    },
  },
})

export type SidebarButtonVariants = RecipeVariants<typeof sidebarButton>
