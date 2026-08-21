import { recipe, style } from '@/wax/css'
import type { RecipeVariants } from '@/wax/css'

import { breakpoints } from '@/styles/theme.css'
import { animation, theme } from '@/wax/theme/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

// Geometry shared by the explicit `isMinimized` variant and the mobile
// breakpoint, so a button collapses to an icon the same way in both cases.
const minimizedGeometry = {
  gap: 0,
  justifyContent: 'center',
  paddingInline: 0,
  width: '34px',
} as const

// Class to mark button as disabled (used for styling child elements)
export const disabledClass = style({})

// Class to mark button as active/selected (used for styling child elements)
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
  base: {
    ...baseStyles,
    // The sidebar always collapses on mobile, regardless of the user's
    // preference, so mirror the minimized geometry via a media query.
    '@media': {
      [MOBILE_QUERY]: minimizedGeometry,
    },
  },

  defaultVariants: {
    disabled: false,
    isActive: false,
    isMinimized: false,
    variant: 'default',
  },

  variants: {
    disabled: {
      false: {},
      true: {
        cursor: 'default',
      },
    },

    isActive: {
      false: {},
      true: {},
    },

    isMinimized: {
      false: {},
      true: minimizedGeometry,
    },

    variant: {
      accent: {
        selectors: {
          '&:focus-visible': {
            outline: `1px solid ${theme.button.primary.focus}`,
          },
          [`&.${activeClass}`]: {
            background: theme.sidebar.buttonAccent.selected,
          },
          [`&:hover:not(.${disabledClass})`]: {
            background: theme.sidebar.buttonAccent.hover,
          },
        },
      },
      default: {
        selectors: {
          '&:focus-visible': {
            outline: `1px solid ${theme.button.primary.focus}`,
          },
          [`&.${activeClass}`]: {
            background: theme.sidebar.button.selected,
          },
          [`&:hover:not(.${disabledClass})`]: {
            background: theme.sidebar.button.hover,
          },
        },
      },
    },
  },
})

export const iconStyles = recipe({
  base: {
    flexShrink: 0,
    selectors: {
      [`.${disabledClass} &`]: {
        color: theme.content.disabled,
      },
    },
    transition: animation.colorTransition,
  },

  defaultVariants: {
    variant: 'default',
  },

  variants: {
    variant: {
      accent: {
        color: theme.content.accentContent.secondary,
        selectors: {
          [`.${activeClass} &`]: {
            color: theme.content.accentContent.primary,
          },
        },
      },
      default: {
        color: theme.content.tertiary,
        selectors: {
          [`.${activeClass} &`]: {
            color: theme.content.primary,
          },
        },
      },
    },
  },
})

export const textStyles = recipe({
  base: {
    ...theme.typography.buttonStrong,
    // Hidden on mobile where the sidebar is collapsed to icons. The label stays
    // in the DOM and buttons carry an explicit `aria-label`, so the accessible
    // name is preserved.
    '@media': {
      [MOBILE_QUERY]: {
        display: 'none',
      },
    },
    lineHeight: '18px', // Makes button 30px tall, rather than 30.xx tall.
    overflow: 'hidden',
    selectors: {
      [`.${disabledClass} &`]: {
        color: theme.content.disabled,
      },
    },
    textOverflow: 'ellipsis',
    transition: animation.colorTransition,
    whiteSpace: 'nowrap',
  },

  defaultVariants: {
    variant: 'default',
  },

  variants: {
    variant: {
      accent: {
        color: theme.content.accentContent.secondary,
        selectors: {
          [`.${activeClass} &`]: {
            color: theme.content.accentContent.primary,
          },
        },
      },
      default: {
        color: theme.content.secondary,
        selectors: {
          [`.${activeClass} &`]: {
            color: theme.content.primary,
          },
        },
      },
    },
  },
})

export type SidebarButtonVariants = RecipeVariants<typeof sidebarButton>
