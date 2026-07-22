import { globalStyle, style } from '@vanilla-extract/css'

import { breakpoints, fontFamily } from '@/styles/theme.css'
import { lightTheme } from '@/wax/theme/theme-light.css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  minHeight: 0,
  overflow: 'hidden',
})

export const header = style({
  alignItems: 'center',
  boxSizing: 'border-box',
  display: 'flex',
  flexShrink: 0,
  gap: 24,
  justifyContent: 'space-between',
  marginInline: 'auto',
  maxWidth: 960,
  paddingBlock: 24,
  paddingInline: 32,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      alignItems: 'flex-start',
      paddingBlock: 20,
      paddingInline: 16,
    },
  },
})

export const headerText = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})

export const statusPanel = style({
  boxSizing: 'border-box',
  flexShrink: 0,
  marginInline: 'auto',
  maxWidth: 960,
  paddingBlock: 16,
  paddingInline: 32,
  width: '100%',
})

export const scroll = style({ flex: 1, minHeight: 0 })

export const content = style({
  boxSizing: 'border-box',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  marginInline: 'auto',
  maxWidth: 960,
  minHeight: '100%',
  paddingBlock: 32,
  paddingInline: 32,
  width: '100%',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      paddingBlock: 24,
      paddingInline: 16,
    },
  },
})

export const list = style({ display: 'flex', flexDirection: 'column', gap: 12 })

export const functionRow = style({
  alignItems: 'center',
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 10,
  display: 'flex',
  gap: 14,
  padding: 16,
})

export const functionIcon = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'flex',
  flex: '0 0 38px',
  height: 38,
  justifyContent: 'center',
})

export const functionDetails = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: 5,
  minWidth: 0,
})

export const functionTitle = style({ alignItems: 'center', display: 'flex', gap: 8 })

export const signature = style({
  color: theme.content.secondary,
  fontFamily: fontFamily.code,
  fontSize: 12,
  lineHeight: '18px',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const actions = style({ alignItems: 'center', display: 'flex', flexShrink: 0, gap: 4 })

export const form = style({ display: 'flex', flexDirection: 'column', gap: 18, paddingTop: 20 })

export const fieldsRow = style({
  display: 'grid',
  gap: 16,
  gridTemplateColumns: '1fr 1fr',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: { gridTemplateColumns: '1fr' },
  },
})

export const field = style({ display: 'flex', flexDirection: 'column', gap: 7 })

const textarea = style({
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.input.stroke.default}`,
  borderRadius: 8,
  boxSizing: 'border-box',
  color: theme.content.primary,
  fontFamily: fontFamily.code,
  fontSize: 13,
  lineHeight: '20px',
  outline: 'none',
  padding: 12,
  resize: 'vertical',
  width: '100%',
  selectors: {
    '&:focus': {
      borderColor: theme.input.stroke.focus,
      boxShadow: `0 0 0 1px ${theme.stroke.focused}`,
    },
    '&::placeholder': { color: theme.content.placeholder },
  },
})

export const descriptionEditor = style([textarea, { minHeight: 72 }])

export const sqlEditorShell = style({
  background: theme.surface.onMainContent,
  borderRadius: 8,
  position: 'relative',
})

export const sqlHighlight = style({
  bottom: 1,
  boxSizing: 'border-box',
  color: theme.content.primary,
  fontFamily: fontFamily.code,
  fontSize: 13,
  left: 1,
  lineHeight: '20px',
  margin: 0,
  overflow: 'hidden',
  padding: 12,
  pointerEvents: 'none',
  position: 'absolute',
  right: 1,
  top: 1,
  whiteSpace: 'pre-wrap',
  wordBreak: 'break-word',
})

export const sqlEditor = style([
  textarea,
  {
    background: 'transparent',
    caretColor: theme.content.primary,
    color: 'transparent',
    minHeight: 220,
    position: 'relative',
    selectors: {
      '&::selection': { background: 'rgba(86, 156, 214, 0.28)' },
    },
  },
])

globalStyle(`${sqlHighlight} .sql-keyword`, { color: '#569CD6', fontWeight: 600 })
globalStyle(`body.${lightTheme} ${sqlHighlight} .sql-keyword`, { color: '#0000FF' })
globalStyle(`${sqlHighlight} .sql-function`, { color: '#4EC9B0' })
globalStyle(`body.${lightTheme} ${sqlHighlight} .sql-function`, { color: '#795E26' })
globalStyle(`${sqlHighlight} .sql-string`, { color: '#CE9178' })
globalStyle(`body.${lightTheme} ${sqlHighlight} .sql-string`, { color: '#A31515' })
globalStyle(`${sqlHighlight} .sql-number`, { color: '#CE9178' })
globalStyle(`body.${lightTheme} ${sqlHighlight} .sql-number`, { color: '#098658' })
globalStyle(`${sqlHighlight} .sql-comment`, { color: '#6A9955', fontStyle: 'italic' })
globalStyle(`body.${lightTheme} ${sqlHighlight} .sql-comment`, { color: '#008000' })
globalStyle(`${sqlHighlight} .sql-identifier`, { color: '#9CDCFE' })
globalStyle(`body.${lightTheme} ${sqlHighlight} .sql-identifier`, { color: '#001080' })
