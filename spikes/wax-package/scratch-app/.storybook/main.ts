// SPIKE (withcoral/coral#3541). A Storybook config with Wax's stories read from the
// installed package instead of `app/wax`.
import type { StorybookConfig } from '@storybook/react-vite'

const config: StorybookConfig = {
  stories: [
    '../src/**/*.stories.@(ts|tsx)',
    '../node_modules/@withcoral/wax/**/*.stories.@(ts|tsx)',
  ],
  addons: ['@storybook/addon-vitest'],
  framework: { name: '@storybook/react-vite', options: {} },
}
export default config
