import { style } from '../../css'

import { theme } from '../../theme/theme.css'

/** Keeps the surrounding text's size and weight, so a link sits in a sentence. */
export const link = style({
  color: theme.content.link,
  textDecoration: 'underline',
  overflowWrap: 'anywhere',
  selectors: {
    '&:hover': { color: theme.content.linkHover },
    '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: 2 },
  },
})
