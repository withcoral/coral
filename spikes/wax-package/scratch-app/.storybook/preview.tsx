import type { Preview, Decorator } from '@storybook/react-vite'
import { useEffect } from 'react'
import { configure } from 'storybook/test'

import '@withcoral/wax/theme/reset.css'
import '@withcoral/wax/theme/global.css'
import { themeComparisonDecorator } from '@withcoral/wax/storybook/theme-comparison'
import { theme } from '@withcoral/wax/theme/theme.css'
import { getThemeClass, useTheme } from '@withcoral/wax/theme/theme-provider'

const withThemeDecorator: Decorator = (Story, context) => {
  let { themeClass } = useTheme()

  const bgParams = context.globals.backgrounds?.value
  if (bgParams) {
    themeClass = getThemeClass(bgParams)
  }

  useEffect(() => {
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

configure({ asyncUtilTimeout: 5000 })

const preview: Preview = {
  decorators: [themeComparisonDecorator, withThemeDecorator],
  initialGlobals: { themeComparison: false },
}

export default preview
