import * as a11yAddonAnnotations from '@storybook/addon-a11y/preview'
import { setProjectAnnotations } from '@storybook/react-vite'
import { themeClass } from '../app/styles/theme.css'
import * as projectAnnotations from './preview'
import { getThemeClass } from '../app/wax/theme/theme-provider'

// Browser-mode component tests expect animation frame APIs to exist.
if (typeof window !== 'undefined' && !window.requestAnimationFrame) {
  window.requestAnimationFrame = (callback) => setTimeout(callback, 0) as unknown as number
  window.cancelAnimationFrame = (id) => clearTimeout(id)
}

// Apply vanilla-extract theme to document body for Storybook tests
document.body.classList.add(themeClass)
document.body.classList.add(getThemeClass('dark'))

// This is an important step to apply the right configuration when testing your stories.
// More info at: https://storybook.js.org/docs/api/portable-stories/portable-stories-vitest#setprojectannotations
setProjectAnnotations([a11yAddonAnnotations, projectAnnotations])
