import { globalStyle, keyframes, style } from '@vanilla-extract/css'

import { utils } from '@/styles/utils'
import { lightTheme } from '@/wax/theme/theme-light.css'
import { theme } from '@/wax/theme/theme.css'

const spin = keyframes({ to: { transform: 'rotate(360deg)' } })
const codeFontFamily = '"Gustan Mono", "Roboto Mono", "SFMono-Regular", "SF Mono", Consolas, "Liberation Mono", Menlo, monospace'
export const root = style({ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 })

export const header = style({
  alignItems: 'center',
  borderBlockEnd: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexShrink: 0,
  height: 56,
  justifyContent: 'space-between',
  overflow: 'hidden',
  paddingBlock: 6,
  paddingInline: 32,
})

export const headerTitle = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  gap: 4,
  minWidth: 0,
  overflow: 'hidden',
  paddingInlineEnd: 24,
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const headerActions = style({ alignItems: 'center', display: 'flex', flexShrink: 0, gap: 4 })
export const inlineSearch = style({
  alignItems: 'center',
  display: 'flex',
  gap: 6,
  width: 360,
})
export const inlineSearchField = style({ flex: 1, minWidth: 0 })
export const disconnectedBanner = style({
  backgroundColor: theme.pill.red.background,
  borderBlockEnd: `1px solid ${theme.pill.red.stroke}`,
  color: theme.pill.red.color,
  paddingBlock: 8,
  paddingInline: 16,
  textAlign: 'center',
})

export const queryScroll = style({ flex: 1, minHeight: 0, overflowX: 'hidden', overflowY: 'auto', paddingBlockEnd: 32 })
export const traceList = style({ display: 'flex', flexDirection: 'column' })

export const fullRow = style({
  alignItems: 'center',
  background: 'transparent',
  border: 'none',
  borderBlockEnd: `1px solid ${theme.stroke.primary}`,
  color: theme.content.primary,
  cursor: 'pointer',
  display: 'flex',
  paddingInline: 24,
  textAlign: 'left',
  width: '100%',
  selectors: { '&:hover': { backgroundColor: theme.surface.onMainContentSubtle } },
})
export const statusDot = style({ borderRadius: '50%', flexShrink: 0, height: 8, marginInline: 8, width: 8, selectors: {
  '&[data-tone="ok"]': { backgroundColor: theme.pill.green.color },
  '&[data-tone="error"]': { backgroundColor: theme.pill.red.color },
  '&[data-tone="running"]': { backgroundColor: theme.pill.blue.color },
}})
export const cell = style({ flexShrink: 0, paddingBlock: 10, paddingInline: 8, whiteSpace: 'nowrap' })
export const cellTimestamp = style({ minWidth: 80 })
export const sqlPreview = style({
  color: utils.opacify(theme.content.primary, 85),
  flex: 1,
  fontFamily: codeFontFamily,
  fontSize: 14,
  lineHeight: 1.65,
  margin: 0,
  minWidth: 0,
  overflow: 'hidden',
  paddingBlock: 10,
  paddingInline: 8,
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
export const sqlInlineCode = style({ font: 'inherit' })
export const cellDuration = style({})
export const connectorPill = style({
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: 999,
  color: theme.content.secondary,
  display: 'inline-flex',
  fontSize: 12,
  lineHeight: '18px',
  paddingBlock: 1,
  paddingInline: 8,
})
export const durationDefault = style({ color: theme.content.tertiary })
export const durationWarning = style({ color: theme.content.warning })

export const statusBar = style({
  alignItems: 'center',
  borderBlockStart: `1px solid ${theme.stroke.primary}`,
  display: 'flex',
  flexShrink: 0,
  height: 28,
  justifyContent: 'space-between',
  paddingInline: 12,
})
export const statusLeft = style({ alignItems: 'center', display: 'flex', gap: 6 })
export const statusRight = style({ alignItems: 'center', display: 'flex', gap: 6 })
export const statusBarDot = style({ borderRadius: '50%', flexShrink: 0, height: 6, width: 6, selectors: {
  '&[data-state="connected"]': { backgroundColor: theme.pill.green.color },
  '&[data-state="disconnected"]': { backgroundColor: theme.pill.red.color },
}})
export const statusSep = style({ backgroundColor: theme.stroke.primary, flexShrink: 0, height: 10, width: 1 })

export const emptyState = style({
  alignItems: 'center',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 20,
  justifyContent: 'center',
  minHeight: 0,
  padding: 32,
  textAlign: 'center',
})
export const emptyStateText = style({ alignItems: 'center', display: 'flex', flexDirection: 'column', gap: 4, maxWidth: 440 })
export const loadingState = style({ alignItems: 'center', display: 'flex', flex: 1, gap: 8, justifyContent: 'center' })
export const spinner = style({ animation: `${spin} 1s linear infinite` })

export const detailRoot = style({ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 })
export const detailEmpty = style({ alignItems: 'center', display: 'flex', flex: 1, flexDirection: 'column', gap: 8, justifyContent: 'center', padding: 32, textAlign: 'center' })
export const detailHeaderActions = style({ alignItems: 'center', display: 'flex', flexShrink: 0, gap: 4 })
export const scrollBody = style({ flex: 1, minHeight: 0, overflow: 'auto' })
export const content = style({ display: 'flex', flexDirection: 'column', gap: 16, padding: 16 })
export const sqlBlock = style({ backgroundColor: theme.surface.main, border: `1px solid ${theme.stroke.primary}`, borderRadius: 8, overflow: 'hidden', position: 'relative' })
export const statGrid = style({ display: 'flex', flexWrap: 'wrap', gap: 12 })
export const statCard = style({ backgroundColor: theme.surface.onMainContent, border: `1px solid ${theme.stroke.secondary}`, borderRadius: 12, display: 'flex', flexDirection: 'column', flexShrink: 0, minWidth: 100, paddingBlock: 12, paddingInline: 16 })
export const tabList = style({ borderBlockEnd: `1px solid ${theme.stroke.primary}`, display: 'flex', gap: 4 })
export const tabTrigger = style({ background: 'none', border: 'none', borderBlockEnd: '2px solid transparent', color: theme.content.tertiary, cursor: 'pointer', fontSize: 14, fontWeight: 500, lineHeight: 1.65, marginBlockEnd: -1, paddingBlock: 8, paddingInline: 12 })
export const tabTriggerActive = style({ borderBlockEndColor: theme.pill.green.color, color: theme.content.primary })
export const tabContent = style({ paddingBlockStart: 16 })

export const waterfallRoot = style({
  alignItems: 'stretch',
  display: 'grid',
  gridTemplateAreas: '"labelAxis timelineAxis" "labels timelineBody"',
  gridTemplateColumns: '220px minmax(0, 1fr)',
  gridTemplateRows: 'auto minmax(0, 1fr)',
  minHeight: 'min(620px, calc(100vh - 360px))',
  paddingBlock: 8,
  paddingInline: 12,
  rowGap: 10,
})
export const waterfallTickRow = style({
  display: 'contents',
})
export const waterfallLabel = style({ borderBlockEnd: `1px solid ${theme.stroke.primary}`, gridArea: 'labelAxis', height: 24, minWidth: 0, paddingBlockEnd: 4 })
export const waterfallTimeline = style({ borderBlockEnd: `1px solid ${theme.stroke.primary}`, gridArea: 'timelineAxis', height: 24, minWidth: 0, overflow: 'hidden', paddingBlockEnd: 4, position: 'relative' })
export const waterfallTick = style({ color: theme.content.tertiary, fontFamily: codeFontFamily, fontSize: 12, lineHeight: '16px', position: 'absolute', whiteSpace: 'nowrap' })
export const waterfallLabelsColumn = style({ display: 'flex', flexDirection: 'column', gap: 10, gridArea: 'labels', minWidth: 0 })
export const waterfallTimelineBody = style({
  alignSelf: 'stretch',
  display: 'flex',
  flexDirection: 'column',
  gap: 10,
  gridArea: 'timelineBody',
  minHeight: 0,
  minWidth: 0,
})
export const waterfallRowShell = style({ borderRadius: 8, minWidth: 0, overflow: 'hidden', position: 'relative' })
export const waterfallRowButton = style({
  alignItems: 'center',
  background: 'none',
  border: 'none',
  borderRadius: 4,
  color: theme.content.primary,
  display: 'flex',
  minHeight: 38,
  padding: 0,
  position: 'relative',
  textAlign: 'left',
  width: '100%',
  selectors: {
    '&[role="button"]': { cursor: 'pointer' },
  },
})
export const waterfallRowHover = style({ backgroundColor: theme.surface.onMainContentSubtle })
export const waterfallSpanLabel = style({
  alignItems: 'center',
  alignSelf: 'stretch',
  display: 'flex',
  justifyContent: 'flex-start',
  minHeight: 38,
  minWidth: 0,
  overflow: 'hidden',
  paddingInlineEnd: 10,
})
export const waterfallSpanLabelActive = style({
  backgroundColor: theme.pill.blue.background,
  boxShadow: `inset 3px 0 0 ${theme.pill.blue.color}`,
  color: theme.content.primary,
})
export const waterfallTreeGuide = style({ borderBlockEnd: `1px solid ${theme.stroke.primary}`, borderInlineStart: `1px solid ${theme.stroke.primary}`, flexShrink: 0, height: 18, marginInlineEnd: 6, width: 10 })
export const waterfallTreeToggle = style({
  alignItems: 'center',
  background: 'none',
  border: 'none',
  borderRadius: 4,
  cursor: 'pointer',
  display: 'inline-flex',
  flexShrink: 0,
  height: 18,
  justifyContent: 'center',
  marginInlineEnd: 3,
  padding: 0,
  paddingInlineEnd: 2,
  selectors: { '&:hover': { backgroundColor: theme.button.bare.hover, color: theme.content.primary } },
})
export const waterfallTreeTogglePlaceholder = style({ flexShrink: 0, marginInlineEnd: 3, width: 38 })
export const waterfallChildCountChip = style({
  alignItems: 'center',
  backgroundColor: theme.pill.gray.background,
  border: `1px solid ${theme.pill.gray.stroke}`,
  borderRadius: 999,
  color: theme.pill.gray.color,
  display: 'inline-flex',
  fontFamily: codeFontFamily,
  fontSize: 10,
  height: 14,
  justifyContent: 'center',
  lineHeight: '12px',
  minWidth: 16,
  paddingInline: 4,
})
export const waterfallPluginPill = style({ alignItems: 'center', display: 'inline-flex', gap: 6, maxWidth: '100%', minWidth: 0, overflow: 'hidden', whiteSpace: 'nowrap' })
export const waterfallPluginDot = style({ borderRadius: '50%', flexShrink: 0, height: 8, width: 8, selectors: {
  '&[data-tone="query"]': { backgroundColor: theme.pill.purple.color },
  '&[data-tone="http"]': { backgroundColor: theme.pill.blue.color },
  '&[data-tone="span"]': { backgroundColor: theme.pill.green.color },
  '&[data-tone="error"]': { backgroundColor: theme.pill.red.color },
}})
export const waterfallLabelText = style({ display: 'flex', flexDirection: 'column', minWidth: 0 })
export const waterfallBarSlot = style({
  alignItems: 'center',
  borderRadius: 4,
  display: 'flex',
  flexShrink: 0,
  minHeight: 38,
  minWidth: 0,
  selectors: {
    '&[role="button"]': { cursor: 'pointer' },
  },
})
export const waterfallBarSlotActive = style({
  boxShadow: `inset 0 0 0 1px ${theme.pill.blue.stroke}`,
})
export const waterfallBarArea = style({ height: 24, minWidth: 0, overflow: 'hidden', position: 'relative', width: '100%' })
export const waterfallBar = style({ alignItems: 'center', borderRadius: 6, display: 'flex', height: 20, insetBlockStart: 2, minWidth: 2, overflow: 'hidden', paddingInline: 8, position: 'absolute', whiteSpace: 'nowrap', selectors: {
  '&[data-tone="query"]': { backgroundColor: theme.pill.purple.background, border: `1px solid ${theme.pill.purple.stroke}`, color: theme.pill.purple.color },
  '&[data-tone="http"]': { backgroundColor: theme.pill.blue.background, border: `1px solid ${theme.pill.blue.stroke}`, color: theme.pill.blue.color },
  '&[data-tone="span"]': { backgroundColor: theme.pill.green.background, border: `1px solid ${theme.pill.green.stroke}`, color: theme.pill.green.color },
  '&[data-tone="error"]': { backgroundColor: theme.pill.red.background, border: `1px solid ${theme.pill.red.stroke}`, color: theme.pill.red.color },
}})
export const waterfallBarLabel = style({ fontFamily: codeFontFamily, fontSize: 12, lineHeight: '16px' })
export const waterfallBarLabelOutside = style({
  color: theme.content.secondary,
  fontFamily: codeFontFamily,
  fontSize: 12,
  lineHeight: '16px',
  maxWidth: '100%',
  overflow: 'hidden',
  position: 'absolute',
  textOverflow: 'ellipsis',
  top: '50%',
  transform: 'translateY(-50%)',
  whiteSpace: 'nowrap',
  selectors: {
    '&[data-align="end"]': { textAlign: 'right' },
  },
})
export const waterfallHttpDetail = style({
  backgroundColor: theme.surface.onMainContentSubtle,
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: 8,
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 10,
  minHeight: 0,
  minWidth: 0,
  overflow: 'hidden',
  paddingBlock: 10,
  paddingInline: 12,
})
export const waterfallHttpTabRow = style({ alignItems: 'center', display: 'flex', gap: 12, justifyContent: 'space-between', minWidth: 0 })
export const httpMetaRow = style({ display: 'flex', flexWrap: 'wrap', gap: 6 })
export const httpMetaChip = style({
  alignItems: 'center',
  backgroundColor: theme.pill.gray.background,
  border: `1px solid ${theme.pill.gray.stroke}`,
  borderRadius: 999,
  color: theme.content.secondary,
  display: 'inline-flex',
  gap: 4,
  paddingBlock: 2,
  paddingInline: 8,
})
export const copyButtonGroup = style({ alignItems: 'center', display: 'flex', flexShrink: 0, gap: 6 })
export const waterfallHttpDetailSection = style({ display: 'flex', flex: 1, flexDirection: 'column', gap: 4, minHeight: 0, minWidth: 0 })

export const statusBadge = style({ borderRadius: 999, display: 'inline-flex', fontSize: 12, paddingBlock: 2, paddingInline: 8, selectors: {
  '&[data-tone="ok"]': { backgroundColor: theme.pill.green.background, color: theme.pill.green.color },
  '&[data-tone="error"]': { backgroundColor: theme.pill.red.background, color: theme.pill.red.color },
  '&[data-tone="running"]': { backgroundColor: theme.pill.blue.background, color: theme.pill.blue.color },
}})
export const emptyPanel = style({ alignItems: 'center', border: `1px dashed ${theme.stroke.primary}`, borderRadius: 8, display: 'flex', justifyContent: 'center', minHeight: 140, padding: 24, textAlign: 'center' })
export const externalCallsList = style({ display: 'flex', flexDirection: 'column', gap: 8 })
export const apiCallsSummary = style({ color: theme.content.tertiary, fontSize: 14, lineHeight: 1.65, paddingBlock: 8, paddingInline: 4 })
export const externalCallCard = style({ border: `1px solid ${theme.stroke.primary}`, borderRadius: 8, overflow: 'hidden' })
export const externalCallButton = style({ alignItems: 'center', background: 'none', border: 'none', color: theme.content.primary, cursor: 'pointer', display: 'flex', gap: 8, lineHeight: 1.65, paddingBlock: 8, paddingInline: 12, textAlign: 'left', width: '100%', selectors: { '&:hover': { backgroundColor: theme.surface.onMainContentSubtle } } })
export const externalCallTableName = style({ fontWeight: 500 })
export const externalCallUrl = style({ flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' })
export const externalCallMeta = style({ flexShrink: 0 })
export const externalCallExpanded = style({ backgroundColor: theme.surface.onMainContentSubtle, borderBlockStart: `1px solid ${theme.stroke.primary}`, display: 'flex', flexDirection: 'column', gap: 8, paddingBlock: 8, paddingInline: 12 })
export const requestUrlRow = style({ alignItems: 'center', display: 'flex', gap: 8 })
export const methodBadge = style({ flexShrink: 0 })
export const requestUrl = style({ flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' })
export const detailsSummary = style({ color: theme.content.tertiary, cursor: 'pointer' })
export const detailsPre = style({
  backgroundColor: theme.surface.main,
  borderRadius: 6,
  flex: 1,
  fontFamily: codeFontFamily,
  fontSize: 12,
  lineHeight: '18px',
  margin: 0,
  marginBlockStart: 4,
  minHeight: 0,
  overflow: 'auto',
  padding: 8,
  whiteSpace: 'pre-wrap',
  wordBreak: 'break-word',
})

globalStyle('.sql-keyword', { color: '#569CD6', fontWeight: 600 })
globalStyle(`body.${lightTheme} .sql-keyword`, { color: '#0000FF' })
globalStyle('.sql-function', { color: '#4EC9B0' })
globalStyle(`body.${lightTheme} .sql-function`, { color: '#795E26' })
globalStyle('.sql-string', { color: '#CE9178' })
globalStyle(`body.${lightTheme} .sql-string`, { color: '#A31515' })
globalStyle('.sql-number', { color: '#CE9178' })
globalStyle(`body.${lightTheme} .sql-number`, { color: '#098658' })
globalStyle('.sql-comment', { color: '#6A9955', fontStyle: 'italic' })
globalStyle(`body.${lightTheme} .sql-comment`, { color: '#008000' })
globalStyle('.sql-identifier', { color: '#9CDCFE' })
globalStyle(`body.${lightTheme} .sql-identifier`, { color: '#001080' })
globalStyle(`${sqlBlock} pre`, { color: theme.content.primary, fontFamily: codeFontFamily, fontSize: 14, lineHeight: 1.65, margin: 0, overflowX: 'auto', padding: 12, whiteSpace: 'pre-wrap', wordBreak: 'break-all' })
