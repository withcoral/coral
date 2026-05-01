import { globalStyle, keyframes, createGlobalTheme } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'
import { lightTheme } from '@/wax/theme/theme-light.css'

// Import wax global styles (fonts, scrollbar, selection)
import '@/wax/theme/global.css'

/**
 * Phoebe semantic colors — extend the wax theme with app-specific tokens.
 * These map to the existing Phoebe palette used throughout the app.
 */
export const phoebeVars = createGlobalTheme(':root', {
  pg: '#6FE794',
  pgLight: '#B8F1C5',
  pgDim: '#316841',
  pgBg: '#182B1D',
  danger: '#E5484D',
  dangerLight: '#FF9592',
  dangerDim: '#8C333A',
  dangerBg: '#3B1219',
  info: '#0090FF',
  infoLight: '#70B8FF',
  infoDim: '#205D9E',
  infoBg: '#0D2847',
  warn: '#FFC53D',
  warnLight: '#FFE7B3',
  warnDim: '#714F19',
  warnBg: '#302008',
  intent: '#AB4ABA',
  intentLight: '#CB81FF',
  intentDim: '#692D6F',
  intentBg: '#341A34',
  notice: '#F76808',
  noticeLight: '#FF8B3E',
  success: '#30A46C',
  successLight: '#3DD68C',
  successDim: '#28684A',
  successBg: '#132D21',
})

// SQL syntax highlighting (VS Code Dark+ inspired)
globalStyle('.sql-keyword', {
  color: '#569CD6',
  fontWeight: 600,
})
globalStyle('.sql-function', {
  color: '#4EC9B0',
})
globalStyle('.sql-string', {
  color: '#CE9178',
})
globalStyle('.sql-number', {
  color: '#CE9178',
})
globalStyle('.sql-comment', {
  color: '#6A9955',
  fontStyle: 'italic',
})
globalStyle('.sql-identifier', {
  color: '#9CDCFE',
})

// SQL syntax highlighting — light-mode overrides (VS Code Light+ inspired)
globalStyle(`.${lightTheme} .sql-keyword`, {
  color: '#0000FF',
})
globalStyle(`.${lightTheme} .sql-function`, {
  color: '#795E26',
})
globalStyle(`.${lightTheme} .sql-string`, {
  color: '#A31515',
})
globalStyle(`.${lightTheme} .sql-number`, {
  color: '#098658',
})
globalStyle(`.${lightTheme} .sql-comment`, {
  color: '#008000',
})
globalStyle(`.${lightTheme} .sql-identifier`, {
  color: '#001080',
})

// Animations
export const slideInFromRight = keyframes({
  from: { transform: 'translateX(100%)' },
  to: { transform: 'translateX(0)' },
})

export const slideInFromTop = keyframes({
  from: { opacity: '0' },
  to: { opacity: '1' },
})

export const pulse = keyframes({
  '0%, 100%': { opacity: '1' },
  '50%': { opacity: '0.5' },
})

// Body base styles
globalStyle('body', {
  backgroundColor: theme.surface.main,
  color: theme.content.primary,
  margin: 0,
  padding: 0,
})

globalStyle('*, *::before, *::after', {
  boxSizing: 'border-box',
})
