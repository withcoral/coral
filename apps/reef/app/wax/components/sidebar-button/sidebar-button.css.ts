import { keyframes, recipe, style } from '@/wax/css'
import type { RecipeVariants } from '@/wax/css'

import { animation, theme } from '@/wax/theme/theme.css'

// Geometry for the explicit `isMinimized` variant. The mobile drawer keeps
// labels, so no breakpoint collapses a button on its own.
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

// Muted shares the default surface; only its resting text is quieter.
const defaultSurface = {
  selectors: {
    '&:focus-visible': {
      outline: `1px solid ${theme.button.primary.focus}`,
    },
    [`&.${activeClass}`]: {
      background: theme.sidebar.button.selected,
    },
    // A menu trigger stays highlighted while its menu is open.
    [`&:hover:not(.${disabledClass}), &[data-popup-open]`]: {
      background: theme.sidebar.button.hover,
    },
  },
}

export const sidebarButton = recipe({
  base: baseStyles,

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
      default: defaultSurface,
      muted: defaultSurface,
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
      muted: {
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
        color: theme.content.muted,
        selectors: {
          [`.${activeClass} &`]: {
            color: theme.content.primary,
          },
        },
      },
      muted: {
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

export const badgeStyles = recipe({
  base: {
    ...theme.typography.bodySmallStrong,
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: '999px',
    flexShrink: 0,
    marginInlineStart: 'auto',
    minWidth: 20,
    height: 20,
    // Overhang the 18px line box, so a badge never makes its row taller.
    marginBlock: -1,
    paddingInline: 5,
    fontVariantNumeric: 'tabular-nums',
    backgroundColor: theme.sidebar.badge.background,
    color: theme.sidebar.badge.content,
  },
  variants: {
    isMinimized: {
      false: {},
      true: {
        position: 'absolute',
        insetBlockStart: 2,
        insetInlineEnd: 2,
        minWidth: 8,
        width: 8,
        height: 8,
        paddingInline: 0,
      },
    },
  },
})

export type SidebarButtonVariants = RecipeVariants<typeof sidebarButton>

// Notification badge (transitions.dev): an arriving badge slides up from the
// lower left while it pops from nothing with a slight bounce, fades in and
// sharpens. A leaving badge shrinks, fades and blurs away quickly.
const badgeHidden = { filter: 'blur(2px)', opacity: 0, scale: '0' }
const badgeSlide = keyframes({ from: { translate: '-8.2px 12.4px' } })
const badgePop = keyframes({ from: { filter: badgeHidden.filter, scale: badgeHidden.scale } })
const badgeFade = keyframes({ from: { opacity: 0 } })
const bounce = 'cubic-bezier(0.34, 1.36, 0.64, 1)'
const settle = 'cubic-bezier(0.4, 0, 0.2, 1)'
const smoothOut = 'cubic-bezier(0.22, 1, 0.36, 1)'

export const badgeEnter = style({
  animation: [
    `${badgeSlide} 260ms ${smoothOut}`,
    `${badgePop} 500ms ${bounce}`,
    `${badgeFade} 400ms ${bounce}`,
  ].join(', '),
  '@media': { '(prefers-reduced-motion: reduce)': { animation: 'none' } },
})

// A keyframe rather than a transition: it always runs and ends, even when it
// interrupts an arrival, so the badge always unmounts.
const badgePopOut = keyframes({ to: badgeHidden })

export const badgeExit = style({
  animation: `${badgePopOut} 180ms ${settle} forwards`,
  // Still end the animation, so the badge unmounts, but too briefly to see.
  '@media': { '(prefers-reduced-motion: reduce)': { animationDuration: '1ms' } },
})
