import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { Controller, useForm } from 'react-hook-form'
import { expect, userEvent } from 'storybook/test'

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

export const Invalid: Story = {
  args: {
    invalid: true,
    placeholder: 'Type something...',
    value: 'not-an-email',
  },
  play: async ({ canvas }) => {
    const input = canvas.getByRole('textbox')

    await expect(input).toHaveAttribute('aria-invalid', 'true')
    await expect(input).toHaveAttribute('data-invalid')
  },
}

export const DescribedError: Story = {
  render: () => (
    <>
      <TextInput
        ariaLabel="Token URL"
        ariaDescribedBy="token-url-error"
        invalid
        value="not-a-url"
      />
      <p id="token-url-error">Enter a valid token URL.</p>
    </>
  ),
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('textbox', { name: 'Token URL' })).toHaveAccessibleDescription(
      'Enter a valid token URL.',
    )
  },
}

export const Uncontrolled: Story = {
  args: {
    placeholder: 'Type something...',
  },
}

export const NativeFormConstraints: Story = {
  args: { name: 'displayName', defaultValue: 'Operations team', required: true, maxLength: 256 },
  play: async ({ canvas }) => {
    const input = canvas.getByRole('textbox')
    await expect(input).toHaveValue('Operations team')
    await expect(input).toBeRequired()
    await expect(input).toHaveAttribute('maxlength', '256')
  },
}

export const RegistrationExpiration: Story = {
  args: {
    ariaLabel: 'Expiration (UTC)',
    name: 'expiration',
    type: 'datetime-local',
    defaultValue: '2026-12-31T23:59:59',
    step: '1',
  },
  play: async ({ canvas }) => {
    const input = canvas.getByLabelText('Expiration (UTC)')
    await expect(input).toHaveValue('2026-12-31T23:59:59')
    await expect(input).toHaveAttribute('step', '1')
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

export const Search: Story = {
  render: () => {
    const [value, setValue] = useState('issues')
    return (
      <TextInput
        ariaLabel="Find a tool"
        icon="Search"
        onChange={setValue}
        placeholder="Find a tool"
        type="search"
        value={value}
      />
    )
  },
  play: async ({ canvas }) => {
    const input = canvas.getByRole('searchbox', { name: 'Find a tool' })
    await userEvent.click(canvas.getByRole('button', { name: 'Clear search' }))

    await expect(input).toHaveValue('')
    await expect(input).toHaveFocus()
    await expect(canvas.queryByRole('button', { name: 'Clear search' })).toBeNull()

    // Ends with text, so the visual snapshot shows the button.
    await userEvent.keyboard('pulls')
    await expect(canvas.getByRole('button', { name: 'Clear search' })).toBeVisible()
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
