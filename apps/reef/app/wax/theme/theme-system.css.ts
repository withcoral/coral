import { assignVars, createGlobalTheme, globalStyle } from '@/wax/css'

import { darkThemeValues } from './theme-dark.css'
import { lightThemeValues } from './theme-light.css'
import { theme } from './theme.css'

// Standalone, script-free documents follow the OS preference using the same
// tokens as the interactive theme classes. Import only at their document entry.
createGlobalTheme(':root', theme, lightThemeValues)
globalStyle(':root', {
  colorScheme: 'light dark',
  '@media': {
    '(prefers-color-scheme: dark)': { vars: assignVars(theme, darkThemeValues) },
  },
})
