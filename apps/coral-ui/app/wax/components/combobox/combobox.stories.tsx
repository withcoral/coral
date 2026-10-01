import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'

import { Combobox } from '@/wax/components'

const fruits = [
  'Apple',
  'Banana',
  'Blueberry',
  'Cherry',
  'Grape',
  'Lemon',
  'Mango',
  'Orange',
  'Peach',
  'Strawberry',
]
const longFruits = [
  'A very long fruit name that should be truncated nicely in chips and list items',
  'Another extraordinarily verbose tropical fruit label for overflow testing',
  'Papaya',
  'Dragon fruit',
]

const meta: Meta<typeof Combobox.Root> = {
  component: Combobox.Root,
  title: 'Wax/Combobox',
}

export default meta
type Story = StoryObj<typeof Combobox.Root>

export const Default: Story = {
  render: () => (
    <div style={{ padding: '100px', width: '360px' }}>
      <Combobox.Root items={fruits}>
        <Combobox.Input placeholder="Select a fruit..." />
        <Combobox.Content>
          <Combobox.Empty />
          <Combobox.List>
            {(fruit) => (
              <Combobox.Item key={fruit} value={fruit}>
                {fruit}
              </Combobox.Item>
            )}
          </Combobox.List>
        </Combobox.Content>
      </Combobox.Root>
    </div>
  ),
}

export const Controlled: Story = {
  render: function Render() {
    const [value, setValue] = useState<string>()

    return (
      <div style={{ padding: '100px', width: '360px' }}>
        <Combobox.Root
          items={fruits}
          onValueChange={(nextValue) =>
            setValue(typeof nextValue === 'string' ? nextValue : undefined)
          }
          value={value}
        >
          <Combobox.Input placeholder="Select a fruit..." />
          <Combobox.Content>
            <Combobox.Empty />
            <Combobox.List>
              {(fruit) => (
                <Combobox.Item key={fruit} value={fruit}>
                  {fruit}
                </Combobox.Item>
              )}
            </Combobox.List>
          </Combobox.Content>
        </Combobox.Root>
        <p style={{ color: 'white', marginTop: '16px' }}>Selected: {value ?? 'none'}</p>
      </div>
    )
  },
}

export const Multiple: Story = {
  render: function Render() {
    const [value, setValue] = useState<string[]>([])

    return (
      <div style={{ padding: '100px', width: '360px' }}>
        <Combobox.Root
          items={fruits}
          multiple
          onValueChange={(nextValue) => setValue(Array.isArray(nextValue) ? nextValue : [])}
          value={value}
        >
          <Combobox.InputGroup>
            <Combobox.Chips>
              <Combobox.Value>
                {(selectedValue) =>
                  Array.isArray(selectedValue)
                    ? selectedValue.map((fruit) => (
                        <Combobox.Chip key={fruit}>
                          <Combobox.ChipLabel>{fruit}</Combobox.ChipLabel>
                          <Combobox.ChipRemove aria-label={`Remove ${fruit}`} />
                        </Combobox.Chip>
                      ))
                    : null
                }
              </Combobox.Value>
              <Combobox.Input bare placeholder={value.length > 0 ? '' : 'Select fruits...'} />
            </Combobox.Chips>
          </Combobox.InputGroup>
          <Combobox.Content>
            <Combobox.Empty />
            <Combobox.List>
              {(fruit) => (
                <Combobox.Item key={fruit} value={fruit}>
                  {fruit}
                </Combobox.Item>
              )}
            </Combobox.List>
          </Combobox.Content>
        </Combobox.Root>
        <p style={{ color: 'white', marginTop: '16px' }}>
          Selected: {value.length ? value.join(', ') : 'none'}
        </p>
      </div>
    )
  },
}

export const MultipleLongLabels: Story = {
  render: function Render() {
    const [value, setValue] = useState<string[]>([longFruits[0], longFruits[1]])

    return (
      <div style={{ padding: '100px', width: '320px' }}>
        <Combobox.Root
          items={longFruits}
          multiple
          onValueChange={(nextValue) => setValue(Array.isArray(nextValue) ? nextValue : [])}
          value={value}
        >
          <Combobox.InputGroup>
            <Combobox.Chips>
              <Combobox.Value>
                {(selectedValue) =>
                  Array.isArray(selectedValue)
                    ? selectedValue.map((fruit) => (
                        <Combobox.Chip key={fruit}>
                          <Combobox.ChipLabel>{fruit}</Combobox.ChipLabel>
                          <Combobox.ChipRemove aria-label={`Remove ${fruit}`} />
                        </Combobox.Chip>
                      ))
                    : null
                }
              </Combobox.Value>
              <Combobox.Input bare placeholder={value.length > 0 ? '' : 'Select fruits...'} />
            </Combobox.Chips>
          </Combobox.InputGroup>
          <Combobox.Content>
            <Combobox.Empty />
            <Combobox.List>
              {(fruit) => (
                <Combobox.Item key={fruit} value={fruit}>
                  {fruit}
                </Combobox.Item>
              )}
            </Combobox.List>
          </Combobox.Content>
        </Combobox.Root>
      </div>
    )
  },
}
