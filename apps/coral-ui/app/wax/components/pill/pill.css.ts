import { style, styleVariants } from '@/wax/css'

import { animation, theme } from '@/wax/theme/theme.css'

export const basePill = style({
  alignItems: 'center',
  cursor: 'default',
  display: 'inline-flex',
  maxWidth: '100%',
  transition: animation.colorTransition,
})

export const interactive = style({
  cursor: 'pointer',
})

export const active = style({})

const interactiveSelector = `${interactive}&:hover`
const activeSelector = `${active}&`

export const sizeVariants = styleVariants({
  default: {
    borderRadius: '6px',
    fontFamily: theme.typography.bodySmallStrong.fontFamily,
    fontSize: '12px',
    fontWeight: theme.typography.bodySmallStrong.fontWeight,
    gap: '4px',
    letterSpacing: theme.typography.bodySmallStrong.letterSpacing,
    lineHeight: '16px',
    paddingBlock: '2px',
    paddingInline: '6px',
  },
  large: {
    borderRadius: '8px',
    fontFamily: theme.typography.body.fontFamily,
    fontSize: theme.typography.body.fontSize,
    fontWeight: theme.typography.body.fontWeight,
    gap: '8px',
    letterSpacing: theme.typography.body.letterSpacing,
    lineHeight: theme.typography.body.lineHeight,
    paddingBlock: '4px',
    paddingInline: '8px',
  },
})

export const textContent = style({
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const colorVariants = styleVariants({
  amber: {
    backgroundColor: theme.pill.amber.background,
    color: theme.pill.amber.color,
    selectors: {
      [activeSelector]: {
        border: `1px solid ${theme.pill.amber.stroke}`,
      },
      [interactiveSelector]: {
        backgroundColor: theme.pill.amber.backgroundHover,
        color: theme.pill.amber.colorHover,
      },
    },
  },
  blue: {
    backgroundColor: theme.pill.blue.background,
    color: theme.pill.blue.color,
    selectors: {
      [activeSelector]: {
        border: `1px solid ${theme.pill.blue.stroke}`,
      },
      [interactiveSelector]: {
        backgroundColor: theme.pill.blue.backgroundHover,
        color: theme.pill.blue.colorHover,
      },
    },
  },
  gray: {
    backgroundColor: theme.pill.gray.background,
    color: theme.pill.gray.color,
    selectors: {
      [activeSelector]: {
        border: `1px solid ${theme.pill.gray.stroke}`,
      },
      [interactiveSelector]: {
        backgroundColor: theme.pill.gray.backgroundHover,
        color: theme.pill.gray.colorHover,
      },
    },
  },
  graySubtle: {
    backgroundColor: theme.pill.graySubtle.background,
    color: theme.pill.graySubtle.color,
    selectors: {
      [activeSelector]: {
        border: `1px solid ${theme.pill.graySubtle.stroke}`,
      },
      [interactiveSelector]: {
        backgroundColor: theme.pill.graySubtle.backgroundHover,
        color: theme.pill.graySubtle.colorHover,
      },
    },
  },
  green: {
    backgroundColor: theme.pill.green.background,
    color: theme.pill.green.color,
    selectors: {
      [activeSelector]: {
        border: `1px solid ${theme.pill.green.stroke}`,
      },
      [interactiveSelector]: {
        backgroundColor: theme.pill.green.backgroundHover,
        color: theme.pill.green.colorHover,
      },
    },
  },
  mention: {
    backgroundColor: theme.pill.mention.background,
    color: theme.pill.mention.color,
    selectors: {
      [activeSelector]: {
        border: `1px solid ${theme.pill.mention.stroke}`,
      },
      [interactiveSelector]: {
        backgroundColor: theme.pill.mention.backgroundHover,
        color: theme.pill.mention.colorHover,
      },
    },
  },
  orange: {
    backgroundColor: theme.pill.orange.background,
    color: theme.pill.orange.color,
    selectors: {
      [activeSelector]: {
        border: `1px solid ${theme.pill.orange.stroke}`,
      },
      [interactiveSelector]: {
        backgroundColor: theme.pill.orange.backgroundHover,
        color: theme.pill.orange.colorHover,
      },
    },
  },
  purple: {
    backgroundColor: theme.pill.purple.background,
    color: theme.pill.purple.color,
    selectors: {
      [activeSelector]: {
        border: `1px solid ${theme.pill.purple.stroke}`,
      },
      [interactiveSelector]: {
        backgroundColor: theme.pill.purple.backgroundHover,
        color: theme.pill.purple.colorHover,
      },
    },
  },
  red: {
    backgroundColor: theme.pill.red.background,
    color: theme.pill.red.color,
    selectors: {
      [activeSelector]: {
        border: `1px solid ${theme.pill.red.stroke}`,
      },
      [interactiveSelector]: {
        backgroundColor: theme.pill.red.backgroundHover,
        color: theme.pill.red.colorHover,
      },
    },
  },
})

// Combined style for mention pills (used in search bar)
export const mentionPill = `${basePill} ${sizeVariants.default} ${colorVariants.mention}`
