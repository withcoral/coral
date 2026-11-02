import { style } from '@vanilla-extract/css'

/** A provider's logo before the call, aligned on the heading's line. */
export const title = style({ display: 'inline-flex', alignItems: 'center', gap: 8 })
export const logo = style({ display: 'inline-flex', flexShrink: 0 })
