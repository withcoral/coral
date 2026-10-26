import type { Preview, Decorator } from '@storybook/react-vite'
import { useEffect } from 'react'

import '../app/wax/theme/reset.css'
import '../app/wax/theme/global.css'
import { theme } from '../app/wax/theme/theme.css'
import { getThemeClass, useTheme } from '../app/wax/theme/theme-provider'
import { themeComparisonDecorator } from './addons/theme-comparison'
import { ShellFormContext } from '../app/views/management/forms'

const withThemeDecorator: Decorator = (Story, context) => {
  let { themeClass } = useTheme()

  const bgParams = context.globals.backgrounds.value
  if (bgParams) {
    themeClass = getThemeClass(bgParams)
  }

  useEffect(() => {
    // Apply theme to document.body so the Popover.Portal can access CSS variables
    document.body.classList.add(themeClass)
    document.body.style.setProperty('background-color', theme.surface.mainContent, 'important')

    return () => {
      document.body.classList.remove(themeClass)
    }
  }, [themeClass])
  return (
    <div className={themeClass}>
      <Story />
    </div>
  )
}

// The app shell provides its form context to every page.
const withShellForm: Decorator = (Story) => (
  <ShellFormContext value={{ csrf: 'storybook', requestId: 'storybook' }}>
    <Story />
  </ShellFormContext>
)

const preview: Preview = {
  decorators: [themeComparisonDecorator, withThemeDecorator, withShellForm],
  initialGlobals: {
    themeComparison: false,
  },
  parameters: {
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },

    a11y: {
      // 'todo' - show a11y violations in the test UI only
      // 'error' - fail CI on a11y violations
      // 'off' - skip a11y checks entirely
      test: 'todo',
    },
  },
}

export default preview
