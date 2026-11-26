import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, within } from 'storybook/test'

import { App } from './app'

const meta = { component: App, title: 'Scratch/App' } satisfies Meta<typeof App>
export default meta

export const Default: StoryObj<typeof meta> = {
  play: async ({ canvasElement }) => {
    const button = within(canvasElement).getByRole('button', { name: 'Save' })
    // A Wax rule reached the page: the button is not painted as a plain one is.
    const plain = canvasElement.appendChild(document.createElement('button'))
    const plainBackground = getComputedStyle(plain).backgroundColor
    plain.remove()
    await expect(getComputedStyle(button).backgroundColor).not.toBe(plainBackground)
  },
}
