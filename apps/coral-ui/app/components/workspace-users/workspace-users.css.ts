import { globalStyle, style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

export const page = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '24px',
  marginInline: 'auto',
  maxWidth: '960px',
  paddingBlock: '32px',
  paddingInline: '24px',
  width: '100%',
  '@media': {
    [MOBILE_QUERY]: { paddingBlock: '20px', paddingInline: '16px' },
  },
})

export const header = style({
  alignItems: 'flex-start',
  display: 'flex',
  gap: '8px',
  '@media': {
    [MOBILE_QUERY]: { flexDirection: 'column', gap: '16px' },
  },
})
export const headerText = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: '4px',
  minWidth: 0,
})
export const headerControls = style({
  alignItems: 'center',
  display: 'flex',
  flex: '0 1 auto',
  gap: '8px',
  marginInlineStart: 'auto',
  '@media': {
    [MOBILE_QUERY]: { marginInlineStart: 0, width: '100%' },
  },
})
export const searchBar = style({ flex: '1 1 280px', maxWidth: '280px', minWidth: '180px' })
export const searchInput = style({})
globalStyle(`${searchInput}::-webkit-search-cancel-button`, { display: 'none' })
export const loadError = style({ alignItems: 'center', display: 'flex', gap: '8px' })
export const memberRow = style({ alignItems: 'flex-start', display: 'flex', gap: '10px' })
export const memberIdentity = style({ display: 'flex', flexDirection: 'column', gap: '4px' })
export const memberNameLine = style({
  alignItems: 'baseline',
  display: 'flex',
  flexWrap: 'wrap',
  gap: '4px 8px',
})
export const memberTableRow = style({})
export const removeButton = style({
  marginInlineStart: 'auto',
  opacity: 1,
  transition: 'opacity 120ms ease',
  '@media': {
    '(hover: hover) and (pointer: fine)': { opacity: 0 },
  },
  selectors: {
    '&:focus-visible': { opacity: 1 },
    [`${memberTableRow}:hover &`]: { opacity: 1 },
  },
})

export const roleTrigger = style({ justifyContent: 'space-between' })
export const roleMenu = style({ maxWidth: 'calc(100vw - 32px)', width: '160px' })
export const addForm = style({ display: 'flex', flexDirection: 'column', gap: '16px' })
export const addFields = style({
  alignItems: 'start',
  display: 'grid',
  gap: '12px',
  gridTemplateColumns: 'minmax(0, 1fr) 140px',
  '@media': {
    [MOBILE_QUERY]: { gridTemplateColumns: 'minmax(0, 1fr)' },
  },
})
export const addField = style({ display: 'flex', flexDirection: 'column', gap: '8px' })
export const addRoleMenu = style({ width: '100%' })
