import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import { utils } from '@/styles/utils'
import { List } from '@/wax/components'
import { theme } from '@/wax/theme/theme.css'

const meta: Meta<typeof List.Container> = {
  component: List.Container,
  decorators: [
    (Story) => {
      return (
        <div style={{ backgroundColor: theme.surface.mainContent, padding: 24 }}>
          <Story />
        </div>
      )
    },
  ],
  title: 'Wax/List',
}

export default meta
type Story = StoryObj<typeof List.Container>

export const Default: Story = {
  render: () => (
    <List.Container>
      <List.Item>
        <List.Title>First item</List.Title>
        <List.Footer>Additional details</List.Footer>
      </List.Item>
      <List.Item>
        <List.Title>Second item</List.Title>
        <List.Footer>More info here</List.Footer>
      </List.Item>
      <List.Item>
        <List.Title>Third item</List.Title>
        <List.Footer>Footer text</List.Footer>
      </List.Item>
    </List.Container>
  ),
}

export const LongText: Story = {
  render: () => (
    <div style={{ width: '400px' }}>
      <List.Container>
        <List.Item>
          <List.Title truncate>
            ThisIsAVeryLongWordWithoutSpacesThatShouldTestTextOverflowHandling
          </List.Title>
          <List.Footer>Short footer</List.Footer>
        </List.Item>
        <List.Item>
          <List.Title>Normal title</List.Title>
          <List.Footer truncate>
            ThisIsAVeryLongFooterTextWithoutAnySpacesThatShouldTestOverflowBehavior
          </List.Footer>
        </List.Item>
        <List.Item>
          <List.Title style={utils.boxClamp(5)}>
            This is a very long title with spaces that should wrap naturally across multiple lines
            to test how the component handles lengthy text content that exceeds the available width.
            This is a very long title with spaces that should wrap naturally across multiple lines
            to test how the component handles lengthy text content that exceeds the available width.
            This is a very long title with spaces that should wrap naturally across multiple lines
            to test how the component handles lengthy text content that exceeds the available width.
          </List.Title>
          <List.Footer style={utils.boxClamp(5)}>
            This is a very long footer with spaces that should also wrap naturally across multiple
            lines to verify proper text wrapping behavior in the footer area. This is a very long
            footer with spaces that should also wrap naturally across multiple lines to verify
            proper text wrapping behavior in the footer area. This is a very long footer with spaces
            that should also wrap naturally across multiple lines to verify proper text wrapping
            behavior in the footer area.
          </List.Footer>
        </List.Item>
      </List.Container>
    </div>
  ),
}

type DifferentElementTypesArgs = React.ComponentProps<typeof List.Container> & {
  onClick: () => void
}

export const DifferentElementTypes: StoryObj<DifferentElementTypesArgs> = {
  args: {
    onClick: fn(),
  },
  render: (args) => (
    <List.Container>
      <List.Item>
        <List.Title>Normal item (div)</List.Title>
        <List.Footer>Default element type</List.Footer>
      </List.Item>
      <List.Item as="button" onClick={args.onClick}>
        <List.Title>Bare button</List.Title>
        <List.Footer>Clickable with minimal styling</List.Footer>
      </List.Item>
      <List.Item interactive onClick={args.onClick}>
        <List.Title>Interactive</List.Title>
        <List.Footer>With the cursor pointer</List.Footer>
      </List.Item>
    </List.Container>
  ),
}
