import { assignVars, createThemeContract, style } from '@vanilla-extract/css'
import { recipe, RecipeVariants } from '@vanilla-extract/recipes'

import { animation, theme } from '@/wax/theme/theme.css'

// Class to mark button as disabled (used for styling child elements)
export const disabledClass = style({})

// CSS variables for dynamic background colors
const buttonVars = createThemeContract({
  backgroundActive: null,
  backgroundBase: null,
  backgroundHover: null,
  shadow: null,
})

// Base button reset and common styles
const baseStyles = {
  alignItems: 'center',
  background: buttonVars.backgroundBase,
  border: 'none',
  boxShadow: buttonVars.shadow,

  cursor: 'pointer',
  display: 'flex',
  flexGrow: 0,
  flexShrink: 0,
  justifyContent: 'center',
  margin: 0,

  outline: 'none',
  padding: 0,

  position: 'relative',

  textDecoration: 'none',

  transition: animation.colorTransition,
} as const

export const button = recipe({
  base: baseStyles,

  compoundVariants: [
    // Padding for size 22
    {
      style: { paddingBlock: '2px', paddingInline: '6px' },
      variants: { hasPrefix: false, hasSuffix: false, isSymbolOnly: false, size: '22' },
    },
    {
      style: { paddingBlock: '2px', paddingInlineEnd: '8px', paddingInlineStart: '6px' },
      variants: { hasPrefix: true, hasSuffix: false, isSymbolOnly: false, size: '22' },
    },
    {
      style: { paddingBlock: '2px', paddingInlineEnd: '6px', paddingInlineStart: '8px' },
      variants: { hasPrefix: false, hasSuffix: true, isSymbolOnly: false, size: '22' },
    },
    {
      style: { paddingBlock: '2px', paddingInlineEnd: '6px', paddingInlineStart: '6px' },
      variants: { hasPrefix: true, hasSuffix: true, isSymbolOnly: false, size: '22' },
    },
    {
      style: { padding: '4px' },
      variants: { isSymbolOnly: true, size: '22' },
    },

    // Padding for size 32
    {
      style: { paddingBlock: '6px', paddingInline: '10px' },
      variants: { hasPrefix: false, hasSuffix: false, isSymbolOnly: false, size: '32' },
    },
    {
      style: { paddingBlock: '6px', paddingInlineEnd: '10px', paddingInlineStart: '8px' },
      variants: { hasPrefix: true, hasSuffix: false, isSymbolOnly: false, size: '32' },
    },
    {
      style: { paddingBlock: '6px', paddingInlineEnd: '8px', paddingInlineStart: '10px' },
      variants: { hasPrefix: false, hasSuffix: true, isSymbolOnly: false, size: '32' },
    },
    {
      style: { paddingBlock: '6px', paddingInlineEnd: '8px', paddingInlineStart: '8px' },
      variants: { hasPrefix: true, hasSuffix: true, isSymbolOnly: false, size: '32' },
    },
    {
      style: { padding: '6px' },
      variants: { isSymbolOnly: true, size: '32' },
    },

    // Padding for size 36
    {
      style: { paddingBlock: '8px', paddingInline: '14px' },
      variants: { hasPrefix: false, hasSuffix: false, isSymbolOnly: false, size: '36' },
    },
    {
      style: { paddingBlock: '8px', paddingInlineEnd: '14px', paddingInlineStart: '12px' },
      variants: { hasPrefix: true, hasSuffix: false, isSymbolOnly: false, size: '36' },
    },
    {
      style: { paddingBlock: '8px', paddingInlineEnd: '12px', paddingInlineStart: '14px' },
      variants: { hasPrefix: false, hasSuffix: true, isSymbolOnly: false, size: '36' },
    },
    {
      style: { paddingBlock: '8px', paddingInlineEnd: '12px', paddingInlineStart: '12px' },
      variants: { hasPrefix: true, hasSuffix: true, isSymbolOnly: false, size: '36' },
    },
    {
      style: { padding: '8px' },
      variants: { isSymbolOnly: true, size: '36' },
    },
    {
      style: { padding: 0 },
      variants: { variant: 'link' },
    },
    {
      style: { padding: 0 },
      variants: { variant: 'linkSubtle' },
    },
    {
      style: { alignItems: 'flex-start' }, // Center icon in the smallest size
      variants: { size: '22', variant: 'linkSubtle' },
    },
    {
      style: { alignItems: 'flex-start' }, // Center icon in the smallest size
      variants: { size: '22', variant: 'link' },
    },

    // Active state should override disabled for background
    {
      style: { background: buttonVars.backgroundActive },
      variants: { disabled: false, isActive: true },
    },

    // Disabled button styles
    {
      style: { background: theme.button.primary.disabled },
      variants: { disabled: true, variant: 'primary' },
    },
    {
      style: { background: 'transparent' },
      variants: { disabled: true, variant: 'secondary' },
    },
    {
      style: { background: 'transparent' },
      variants: { disabled: true, variant: 'bare' },
    },
    {
      style: { background: theme.button.primary.disabled },
      variants: { disabled: true, variant: 'destructive' },
    },
  ],

  defaultVariants: {
    disabled: false,
    fullWidth: false,
    hasPrefix: false,
    hasSuffix: false,
    isActive: false,
    isSymbolOnly: false,
    size: '32',
    variant: 'primary',
  },

  variants: {
    disabled: {
      false: {
        selectors: {
          '&:hover': {
            background: buttonVars.backgroundHover,
          },
        },
      },
      true: {
        cursor: 'default',
      },
    },

    fullWidth: {
      false: {},
      true: {
        width: '100%',
      },
    },

    hasPrefix: {
      false: {},
      true: {},
    },

    hasSuffix: {
      false: {},
      true: {},
    },

    isActive: {
      false: {},
      true: {
        background: buttonVars.backgroundActive,
      },
    },

    isSymbolOnly: {
      false: {},
      true: {},
    },

    // When adding a new size, we'll need new compound variants too
    size: {
      '22': {
        borderRadius: '5px',
        gap: '6px',
      },
      '32': {
        borderRadius: '8px',
        gap: '8px',
      },
      '36': {
        borderRadius: '10px',
        gap: '8px',
      },
    },

    variant: {
      bare: {
        selectors: {
          '&:focus-visible': {
            outline: `1px solid ${theme.button.primary.focus}`,
          },
        },
        vars: assignVars(buttonVars, {
          backgroundActive: theme.button.bare.hover,
          backgroundBase: 'transparent',
          backgroundHover: 'transparent',
          shadow: 'none',
        }),
      },
      destructive: {
        selectors: {
          '&:focus-visible': {
            outline: `1px solid ${theme.button.destructive.default}`,
            outlineOffset: '1px',
          },
        },
        vars: assignVars(buttonVars, {
          backgroundActive: theme.button.destructive.focus,
          backgroundBase: theme.button.destructive.default,
          backgroundHover: theme.button.destructive.hover,
          shadow: 'none',
        }),
      },
      link: {
        display: 'inline-flex',
        gap: '4px',
        padding: 0,
        selectors: {
          '&:focus-visible': {
            outline: `1px solid ${theme.button.primary.focus}`,
            outlineOffset: '1px',
          },
        },
        vars: assignVars(buttonVars, {
          backgroundActive: 'transparent',
          backgroundBase: 'transparent',
          backgroundHover: 'transparent',
          shadow: 'none',
        }),
      },
      linkSubtle: {
        display: 'inline-flex',
        gap: '4px',
        padding: 0,
        selectors: {
          '&:focus-visible': {
            outline: `1px solid ${theme.button.primary.focus}`,
            outlineOffset: '1px',
          },
        },
        vars: assignVars(buttonVars, {
          backgroundActive: 'transparent',
          backgroundBase: 'transparent',
          backgroundHover: 'transparent',
          shadow: 'none',
        }),
      },
      primary: {
        selectors: {
          '&:focus-visible': {
            outline: `1px solid ${theme.button.primary.focus}`,
            outlineOffset: '1px',
          },
        },
        vars: assignVars(buttonVars, {
          backgroundActive: theme.button.primary.focus,
          backgroundBase: theme.button.primary.default,
          backgroundHover: theme.button.primary.hover,
          shadow: 'none',
        }),
      },
      secondary: {
        selectors: {
          '&:focus-visible': {
            outline: `1px solid ${theme.button.primary.focus}`,
          },
        },
        vars: assignVars(buttonVars, {
          backgroundActive: theme.button.secondary.focus,
          backgroundBase: theme.button.secondary.default,
          backgroundHover: theme.button.secondary.hover,
          shadow: 'none',
        }),
      },
    },
  },
})

export type ButtonSize = NonNullable<ButtonVariants>['size']
export type ButtonVariant = NonNullable<ButtonVariants>['variant']
export type ButtonVariants = RecipeVariants<typeof button>
