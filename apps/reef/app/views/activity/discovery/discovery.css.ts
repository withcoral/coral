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

// An icon-only policy marker sits on the title line like its pills.
export const approval = style({ display: 'inline-flex', alignItems: 'center' })
