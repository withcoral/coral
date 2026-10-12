import { style, styleVariants } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const code = style({
  minWidth: 0,
  maxWidth: '100%',
  flexShrink: 0,
  whiteSpace: 'pre-wrap',
  overflowWrap: 'anywhere',
  tabSize: 2,
})

export const FRAME_PADDING = 12

const frame = {
  background: theme.surface.onMainContentSubtle,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
}

export const framed = style({
  ...frame,
  margin: 0,
  overflowX: 'auto',
  padding: FRAME_PADDING,
  whiteSpace: 'pre',
})

/** The scroll area of a capped block. Its line height is the code's, so `lh` counts code lines. */
export const capped = style({ ...theme.typography.code, flexShrink: 0, minWidth: 0 })
/** A capped block keeps its frame still: only the code inside scrolls. */
export const cappedFrame = style(frame)
export const cappedFramedCode = style({
  margin: 0,
  padding: FRAME_PADDING,
  whiteSpace: 'pre',
})
/** A copyable block: the frame holds the scrolling code and, beside it, the copy button. */
export const copyFrame = style({
  ...frame,
  alignItems: 'start',
  display: 'grid',
  gridTemplateColumns: 'minmax(0, 1fr) auto',
  minWidth: 0,
})
const codeLine = `${theme.typography.code.fontSize} * ${theme.typography.code.lineHeight}`
/** Centres the 32px button on the first line of code. */
export const copyButton = style({
  margin: `calc((${FRAME_PADDING * 2}px + ${codeLine} - 32px) / 2)`,
})
/** After both framed styles, so `wrap` wins in a capped block too. */
export const framedWrap = style({ whiteSpace: 'pre-wrap' })

const syntax = theme.content.code.syntax

export const token = styleVariants({
  comment: { color: syntax.comment },
  constant: { color: syntax.constant },
  function: { color: syntax.function },
  keyword: { color: syntax.keyword },
  string: { color: syntax.string },
  type: { color: syntax.type },
})
