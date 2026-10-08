import type { Decorator } from '@storybook/react-vite'

import { darkTheme } from '@/wax/theme/theme-dark.css'
import { lightTheme } from '@/wax/theme/theme-light.css'
import { theme } from '@/wax/theme/theme.css'

export const themeComparisonDecorator: Decorator = (Story, context) => {
  const isComparisonActive = context.globals.themeComparison

  if (!isComparisonActive) {
    return <Story />
  }

  return (
    <div style={{ display: 'flex', gap: '48px', width: '100%' }}>
      <div
        className={darkTheme}
        style={{
          backgroundColor: theme.surface.mainContent,
          borderRadius: '8px',
          flex: 1,
          padding: '24px',
        }}
      >
        <h3
          style={{
            color: theme.content.primary,
            fontSize: '14px',
            fontWeight: 600,
            marginBottom: '16px',
          }}
        >
          Dark Theme
        </h3>
        <Story />
      </div>
      <div
        className={lightTheme}
        style={{
          backgroundColor: theme.surface.mainContent,
          borderRadius: '8px',
          flex: 1,
          padding: '24px',
        }}
      >
        <h3
          style={{
            color: theme.content.primary,
            fontSize: '14px',
            fontWeight: 600,
            marginBottom: '16px',
          }}
        >
          Light Theme
        </h3>
        <Story />
      </div>
    </div>
  )
}
