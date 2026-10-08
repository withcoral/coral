import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { Controller, useForm } from 'react-hook-form'

import { Button } from '@/wax/components'

import { DurationPicker } from './duration-picker'

const meta = {
  component: DurationPicker,
  title: 'Wax/DurationPicker',
} satisfies Meta<typeof DurationPicker>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    icon: 'ClockCheck',
    label: 'Time to resolve',
  },
  render: (args) => {
    const [value, setValue] = useState<null | number>(null)
    return <DurationPicker icon={args.icon} label={args.label} onClose={setValue} value={value} />
  },
}

/** Controlled component with external value manipulation */
export const Controlled: Story = {
  args: {
    icon: 'ClockCheck',
    label: 'Time to resolve',
  },
  render: (args) => {
    const [value, setValue] = useState<null | number>(4 * 3600) // 4h

    const addHour = () => setValue((prev) => (prev ?? 0) + 3600)
    const removeHour = () => setValue((prev) => Math.max(0, (prev ?? 0) - 3600))

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <DurationPicker icon={args.icon} label={args.label} onClose={setValue} value={value} />
        <div style={{ display: 'flex', gap: 8 }}>
          <Button.Container onClick={removeHour} variant="secondary">
            <Button.Text>- 1 hour</Button.Text>
          </Button.Container>
          <Button.Container onClick={addHour} variant="secondary">
            <Button.Text>+ 1 hour</Button.Text>
          </Button.Container>
        </div>
      </div>
    )
  },
}

/** Uncontrolled - component manages its own state */
export const Uncontrolled: Story = {
  args: {
    icon: 'ClockCheck',
    label: 'Time to resolve',
  },
}

/** Use the DurationPicker in a form with react-hook-form */
export const InForm: Story = {
  args: {
    icon: 'ClockSearch',
    label: 'Time to detect',
  },
  render: (args) => {
    const { control, handleSubmit } = useForm<{ timeToResolve: null | number }>({
      defaultValues: { timeToResolve: null },
    })

    const onSubmit = (data: { timeToResolve: null | number }) => {
      alert(`Submitted: ${data.timeToResolve} seconds`)
    }

    return (
      <form
        onSubmit={handleSubmit(onSubmit)}
        style={{ display: 'flex', flexDirection: 'column', gap: 16 }}
      >
        <Controller
          control={control}
          name="timeToResolve"
          render={({ field }) => (
            <DurationPicker
              icon={args.icon}
              label={args.label}
              onClose={field.onChange}
              value={field.value}
            />
          )}
        />
        <Button.Container type="submit">
          <Button.Text>Submit</Button.Text>
        </Button.Container>
      </form>
    )
  },
}
