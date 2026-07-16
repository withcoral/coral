import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { fontFamily } from '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

export const page = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  overflow: 'auto',
  paddingBlock: '32px',
  paddingInline: '24px',
  '@media': {
    [MOBILE_QUERY]: {
      paddingBlock: '20px',
      paddingInline: '16px',
    },
  },
})

export const container = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '32px',
  marginInline: 'auto',
  maxWidth: '960px',
  width: '100%',
  '@media': {
    [MOBILE_QUERY]: {
      gap: '24px',
    },
  },
})

export const header = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '4px',
})

export const connectCard = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '12px',
  display: 'flex',
  flexDirection: 'column',
  gap: '12px',
  padding: '20px',
})

export const connectCardHead = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '4px',
})

export const commandRow = style({
  alignItems: 'center',
  display: 'flex',
  gap: '8px',
})

export const commandBox = style({
  background: theme.surface.main,
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: '8px',
  color: theme.content.primary,
  flex: 1,
  fontFamily: fontFamily.dmMono,
  fontSize: '13px',
  lineHeight: 1.6,
  minWidth: 0,
  overflowX: 'auto',
  paddingBlock: '10px',
  paddingInline: '12px',
  whiteSpace: 'nowrap',
})

export const section = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '16px',
})

export const sectionHead = style({
  alignItems: 'baseline',
  display: 'flex',
  gap: '8px',
})

export const cardGrid = style({
  display: 'grid',
  gap: '16px',
  gridTemplateColumns: 'repeat(auto-fill, minmax(min(340px, 100%), 1fr))',
})

export const clientCard = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '12px',
  display: 'flex',
  flexDirection: 'column',
  gap: '12px',
  minHeight: '148px',
  padding: '16px',
})

export const cardHeader = style({
  alignItems: 'center',
  display: 'flex',
  gap: '10px',
  minWidth: 0,
})

export const cardTitle = style({
  minWidth: 0,
})

export const path = style({
  display: 'block',
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const cardFooter = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: '8px',
  justifyContent: 'flex-end',
  marginBlockStart: 'auto',
})

export const status = style({
  minHeight: '18px',
})
