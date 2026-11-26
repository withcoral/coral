import { popup as menuPopup } from '../menu/menu.css'
import { globalStyle, style } from '../../css'
import { theme } from '../../theme/theme.css'

export const label = style({
  paddingBlockEnd: '6px',
  paddingInline: '8px',
})

// Fits the longest option instead of the menu's fixed width, and never sits
// narrower than the trigger or wider than the viewport allows.
export const popup = style({
  selectors: {
    [`${menuPopup}&`]: {
      maxWidth: 'min(360px, var(--available-width))',
      minWidth: 'max(180px, var(--anchor-width))',
      width: 'max-content',
    },
  },
})

// A secondary button has no border, so an inset ring draws the error border
// that a text input draws.
export const invalid = style({
  boxShadow: `inset 0 0 0 1px ${theme.input.stroke.error}`,
})

// A trigger that fills its cell keeps its icon and label at the start and the
// chevron at the end, so a column of them lines up.
export const fill = style({
  justifyContent: 'flex-start',
  textAlign: 'start',
  width: '100%',
})
globalStyle(`${fill} > :last-child`, { marginInlineStart: 'auto' })

// An option's label that takes width and no height or text: generated content
// stays out of the trigger's text, and hidden it stays out of its name.
export const widthOf = style({
  display: 'block',
  height: 0,
  overflow: 'hidden',
  visibility: 'hidden',
  '::before': { content: 'attr(data-label)' },
})
