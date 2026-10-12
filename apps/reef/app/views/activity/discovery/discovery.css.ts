import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const stack = style({ display: 'flex', flexDirection: 'column', gap: 12, minWidth: 0 })
export const row = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  gap: 6,
  minWidth: 0,
})

/** Subject, state, detail: one row per kind of result. */
export const coverage = style({
  display: 'grid',
  gridTemplateColumns: 'max-content max-content 1fr',
  alignItems: 'center',
  columnGap: 12,
  rowGap: 6,
  padding: '10px 12px',
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 10,
  background: theme.surface.main,
  minWidth: 0,
})
export const coverageWarning = style({
  borderColor: theme.content.warning,
  background: theme.content.warningBackground,
})
export const coverageError = style({
  borderColor: theme.content.error,
  background: theme.content.errorBackground,
})
export const coverageNote = style({ gridColumn: '3' })

export const hits = style({
  listStyle: 'none',
  margin: 0,
  padding: 0,
  display: 'flex',
  flexDirection: 'column',
})
export const hit = style({
  display: 'grid',
  gridTemplateColumns: '28px minmax(0, 1fr)',
  columnGap: 10,
  paddingBlock: 12,
  borderTop: `1px solid ${theme.stroke.secondary}`,
  selectors: { '&:first-child': { borderTop: 'none' } },
})
export const rank = style({
  ...theme.typography.codeInline,
  color: theme.content.tertiary,
  textAlign: 'right',
  paddingTop: 2,
})
export const hitBody = style({ display: 'flex', flexDirection: 'column', gap: 6, minWidth: 0 })
/** A snippet is cut Markdown shown as text: keep its line breaks, show three lines. */
export const snippet = style({
  display: '-webkit-box',
  WebkitBoxOrient: 'vertical',
  WebkitLineClamp: 3,
  overflow: 'hidden',
  whiteSpace: 'pre-wrap',
  overflowWrap: 'anywhere',
})

export const facts = style({
  display: 'grid',
  gridTemplateColumns: 'max-content 1fr',
  columnGap: 12,
  rowGap: 4,
  alignItems: 'baseline',
  margin: 0,
})
export const fact = style({ margin: 0, minWidth: 0 })
export const diagnostics = style({
  listStyle: 'none',
  margin: 0,
  padding: 0,
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
})
