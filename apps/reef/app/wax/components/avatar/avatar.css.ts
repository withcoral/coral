import { recipe, style } from '@/wax/css'
import type { RecipeVariants } from '@/wax/css'

import { animation, theme } from '@/wax/theme/theme.css'

export const avatarContainer = recipe({
  base: {
    alignItems: 'center',
    borderRadius: '100%',
    display: 'inline-flex',
    flexShrink: 0,
    justifyContent: 'center',
    overflow: 'hidden',
    position: 'relative',
    userSelect: 'none',
    verticalAlign: 'top',
  },

  defaultVariants: {
    shape: 'circle',
    size: '20',
  },

  variants: {
    // A rounded square names a place, such as a Workspace, rather than a person.
    shape: {
      circle: {},
      square: { borderRadius: 8 },
    },
    size: {
      '20': { height: '20px', width: '20px' },
      '28': { height: '28px', width: '28px' },
    },
  },
})

export const avatarBorderOverlay = style({
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: 'inherit',
  display: 'block',
  inset: 0,
  pointerEvents: 'none',
  position: 'absolute',
})

export const avatarImage = style({
  borderRadius: 'inherit',
  height: '100%',
  objectFit: 'cover',
  width: '100%',
})

export const avatarFallback = recipe({
  base: {
    alignItems: 'center',
    cursor: 'default',
    display: 'flex',
    fontWeight: 500,
    height: '100%',
    justifyContent: 'center',
    lineHeight: 1,
    textTransform: 'uppercase',
    transition: animation.colorTransition,
    userSelect: 'none',
    width: '100%',
  },

  defaultVariants: {
    size: '20',
  },

  variants: {
    color: {
      ...theme.avatarFallback,
    },
    size: {
      '20': { fontSize: '10px' },
      '28': { fontSize: '13px' },
    },
  },
})

export type AvatarContainerVariants = RecipeVariants<typeof avatarContainer>
export type AvatarFallbackVariants = RecipeVariants<typeof avatarFallback>
