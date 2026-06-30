import { useAtom } from 'jotai'
import { atomWithStorage } from 'jotai/utils'
import { useSyncExternalStore } from 'react'

import { darkTheme } from '@/wax/theme/theme-dark.css'
import { lightTheme } from '@/wax/theme/theme-light.css'

type Theme = 'dark' | 'light'
type ThemePreference = 'dark' | 'light' | 'system'

export const THEME_STORAGE_KEY = 'coral:theme'
export const themeAtom = atomWithStorage<ThemePreference>(THEME_STORAGE_KEY, 'system')

export function getThemeClass(theme: Theme) {
  return theme === 'light' ? lightTheme : darkTheme
}

export function useTheme() {
  const [themePreference, setTheme] = useAtom(themeAtom)
  const systemTheme = useSystemTheme()

  const resolvedTheme: Theme = themePreference === 'system' ? systemTheme : themePreference

  return {
    setTheme,
    theme: resolvedTheme,
    themeClass: getThemeClass(resolvedTheme),
    themePreference,
  }
}

function getSystemTheme(): Theme {
  if (typeof window === 'undefined') {
    return 'dark'
  }
  return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'
}

function subscribeToSystemTheme(callback: () => void): () => void {
  if (typeof window === 'undefined') {
    return () => {
      console.warn('Failed to subscribe to system theme')
    }
  }
  const mediaQuery = window.matchMedia('(prefers-color-scheme: light)')
  mediaQuery.addEventListener('change', callback)
  return () => mediaQuery.removeEventListener('change', callback)
}

function useSystemTheme(): Theme {
  return useSyncExternalStore(subscribeToSystemTheme, getSystemTheme, () => 'dark')
}
