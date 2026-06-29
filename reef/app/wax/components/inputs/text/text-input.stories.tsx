import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { Controller, useForm } from 'react-hook-form'

import { Button } from '@/wax/components'

import { TextInput } from './text-input'

const meta = {
  component: TextInput,
  decorators: [
    (Story) => (
      <div style={{ width: 300 }}>
        <Story />
      </div>
    ),
  ],
  title: 'Wax/Inputs/TextInput',
} satisfies Meta<typeof TextInput>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    icon: 'Search',
    placeholder: 'Search...',
  },
}

export const Disabled: Story = {
  args: {
    disabled: true,
    placeholder: 'Search...',
  },
}

export const Uncontrolled: Story = {
  args: {
    placeholder: 'Type something...',
  },
}

export const Controlled: Story = {
  render: () => {
    const [value, setValue] = useState(
      "This is a very very very very long text! It surely will overflow, right? And, best of all, it was typed all by hand by Alberto. That's so 2025!",
    )
    return <TextInput onChange={setValue} placeholder="Type something..." value={value} />
  },
}

export const WithReactHookForm: Story = {
  render: () => {
    const { control, handleSubmit } = useForm<{ email: string; name: string }>({
      defaultValues: { email: '', name: '' },
    })

    const onSubmit = (data: { email: string; name: string }) => {
      alert(`Submitted: name=${data.name}, email=${data.email}`)
    }

    return (
      <form
        onSubmit={handleSubmit(onSubmit)}
        style={{ display: 'flex', flexDirection: 'column', gap: 16 }}
      >
        <label>
          Name
          <Controller
            control={control}
            name="name"
            render={({ field }) => (
              <TextInput onChange={field.onChange} placeholder="Enter name" value={field.value} />
            )}
          />
        </label>
        <label>
          Email
          <Controller
            control={control}
            name="email"
            render={({ field }) => (
              <TextInput
                onChange={field.onChange}
                placeholder="Enter email"
                type="email"
                value={field.value}
              />
            )}
          />
        </label>
        <Button.Container type="submit">
          <Button.Text>Submit</Button.Text>
        </Button.Container>
      </form>
    )
  },
}
