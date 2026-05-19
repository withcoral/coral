import { useEffect, useSyncExternalStore } from 'react'

import { darkTheme } from '@/wax/theme/theme-dark.css'
import { lightTheme } from '@/wax/theme/theme-light.css'

type Theme = 'dark' | 'light'
type ThemePreference = 'dark' | 'light' | 'system'

const STORAGE_KEY = 'phoebe:theme'

export function getThemeClass(theme: Theme) {
  return theme === 'light' ? lightTheme : darkTheme
}

export function useTheme() {
  const themePreference = getThemePreference()
  const systemTheme = useSystemTheme()
  const resolvedTheme: Theme = themePreference === 'system' ? systemTheme : themePreference

  const setTheme = (nextTheme: ThemePreference) => {
    window.localStorage.setItem(STORAGE_KEY, nextTheme)
    window.dispatchEvent(new StorageEvent('storage', { key: STORAGE_KEY, newValue: nextTheme }))
  }

  return { setTheme, theme: resolvedTheme, themeClass: getThemeClass(resolvedTheme), themePreference }
}

export function useThemeClassOnBody() {
  const { themeClass } = useTheme()

  useEffect(() => {
    document.body.classList.add(themeClass)
    return () => document.body.classList.remove(themeClass)
  }, [themeClass])
}

function getThemePreference(): ThemePreference {
  if (typeof window === 'undefined') return 'system'
  const stored = window.localStorage.getItem(STORAGE_KEY)
  return stored === 'dark' || stored === 'light' || stored === 'system' ? stored : 'system'
}

function getSystemTheme(): Theme {
  if (typeof window === 'undefined') {
    return 'dark'
  }
  return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'
}

function subscribeToSystemTheme(callback: () => void): () => void {
  if (typeof window === 'undefined') {
    return () => {}
  }
  const mediaQuery = window.matchMedia('(prefers-color-scheme: light)')
  mediaQuery.addEventListener('change', callback)
  window.addEventListener('storage', callback)
  return () => {
    mediaQuery.removeEventListener('change', callback)
    window.removeEventListener('storage', callback)
  }
}

function useSystemTheme(): Theme {
  return useSyncExternalStore(subscribeToSystemTheme, getSystemTheme, () => 'dark')
}
