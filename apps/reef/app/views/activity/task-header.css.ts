import { style } from '@vanilla-extract/css'

/** The Task's name and state, over its facts. */
export const header = style({ display: 'flex', flexDirection: 'column', gap: 8, minWidth: 0 })
/** The agent's feedback keeps its line breaks. */
export const feedback = style({ whiteSpace: 'pre-wrap' })
