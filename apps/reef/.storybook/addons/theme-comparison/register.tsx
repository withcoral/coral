import { addons, types } from 'storybook/manager-api'
import { MoonIcon, SunIcon } from '@storybook/icons'
import React from 'react'

const ADDON_ID = 'theme-comparison'
const TOOL_ID = `${ADDON_ID}/tool`
const PARAM_KEY = 'themeComparison'

addons.register(ADDON_ID, (api) => {
  addons.add(TOOL_ID, {
    type: types.TOOL,
    title: 'Theme Comparison',
    render: () => {
      const [active, setActive] = React.useState(false)

      const toggleComparison = () => {
        const newValue = !active
        setActive(newValue)
        api.updateGlobals({ [PARAM_KEY]: newValue })
      }

      return (
        <button
          type="button"
          aria-pressed={active}
          onClick={toggleComparison}
          title="Toggle theme comparison view"
          style={{
            alignItems: 'center',
            background: active ? 'var(--color-secondary)' : 'transparent',
            border: 0,
            borderRadius: 4,
            color: 'inherit',
            cursor: 'pointer',
            display: 'inline-flex',
            gap: 2,
            height: 28,
            justifyContent: 'center',
            padding: '0 7px',
          }}
        >
          <SunIcon />
          <MoonIcon />
        </button>
      )
    },
  })
})
