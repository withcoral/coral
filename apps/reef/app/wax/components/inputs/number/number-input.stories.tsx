import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { Controller, useForm } from 'react-hook-form'

import { Button } from '@/wax/components'

import { NumberInput } from './number-input'

const meta = {
  component: NumberInput,
  decorators: [
    (Story) => (
      <div style={{ width: 300 }}>
        <Story />
      </div>
    ),
  ],
  title: 'Wax/Inputs/NumberInput',
} satisfies Meta<typeof NumberInput>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    placeholder: '0',
  },
}

export const Disabled: Story = {
  args: {
    disabled: true,
    placeholder: '0',
    value: 42,
  },
}

export const Uncontrolled: Story = {
  args: {
    placeholder: '0',
  },
}

export const Controlled: Story = {
  render: () => {
    const [value, setValue] = useState<null | number>(10)
    return <NumberInput onChange={setValue} placeholder="0" value={value} />
  },
}

/** Using NumberInput with react-hook-form */
export const WithReactHookForm: Story = {
  render: () => {
    const { control, handleSubmit } = useForm<{ price: null | number; quantity: null | number }>({
      defaultValues: { price: null, quantity: null },
    })

    const onSubmit = (data: { price: null | number; quantity: null | number }) => {
      alert(`Submitted: quantity=${data.quantity}, price=${data.price}`)
    }

    return (
      <form
        onSubmit={handleSubmit(onSubmit)}
        style={{ display: 'flex', flexDirection: 'column', gap: 16 }}
      >
        <label>
          Quantity
          <Controller
            control={control}
            name="quantity"
            render={({ field }) => (
              <NumberInput
                onChange={field.onChange}
                placeholder="Enter quantity"
                value={field.value}
              />
            )}
          />
        </label>
        <label>
          Price
          <Controller
            control={control}
            name="price"
            render={({ field }) => (
              <NumberInput
                onChange={field.onChange}
                placeholder="Enter price"
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
