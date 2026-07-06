import type { Meta, StoryObj } from '@storybook/react-vite'
import type { DateValue } from 'react-aria-components'

import { CalendarDate, getLocalTimeZone, parseDate, today } from '@internationalized/date'
import { useState } from 'react'

import { Button } from '@/wax/components'

import { DatePicker } from './index'

const meta: Meta<typeof DatePicker> = {
  component: DatePicker,
  title: 'Wax/DatePicker',
}

export default meta
type Story = StoryObj<typeof DatePicker>

export const Default: Story = {
  render: () => (
    <div style={{ padding: '100px' }}>
      <DatePicker label="Start date" />
    </div>
  ),
}

/** Use `defaultValue` with a date from `@internationalized/date` */
export const WithDefaultValue: Story = {
  render: () => (
    <div style={{ padding: '100px' }}>
      <DatePicker defaultValue={parseDate('2025-01-15')} label="Start date" />
    </div>
  ),
}

/** Use `today(getLocalTimeZone())` to default to today's date */
export const DefaultToToday: Story = {
  render: () => (
    <div style={{ padding: '100px' }}>
      <DatePicker defaultValue={today(getLocalTimeZone())} label="Start date" />
    </div>
  ),
}

/** Use `value` and `onChange` for controlled state */
export const Controlled: Story = {
  render: function ControlledStory() {
    const [date, setDate] = useState<DateValue | null>(parseDate('2025-06-20'))

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: '16px', padding: '100px' }}>
        <DatePicker label="Start date" onChange={setDate} value={date} />
        <p style={{ color: 'white' }}>
          Selected: {date ? date.toDate(getLocalTimeZone()).toLocaleDateString() : 'None'}
        </p>
      </div>
    )
  },
}

/** Use the `name` prop and hidden input for form submission */
export const InForm: Story = {
  render: () => {
    const [value, setValue] = useState<CalendarDate | null>(today(getLocalTimeZone()))

    return (
      <div style={{ padding: '100px' }}>
        <form
          onSubmit={(e) => {
            e.preventDefault()
            const formData = new FormData(e.currentTarget)
            alert(`Submitted: ${formData.get('startDate')}`)
          }}
          style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}
        >
          <DatePicker label="Start date" onChange={setValue} value={value} />

          {/* native form field */}
          <input name="startDate" type="hidden" value={value?.toString() ?? ''} />

          <Button.TextButton type="submit">Submit</Button.TextButton>
        </form>
      </div>
    )
  },
}
