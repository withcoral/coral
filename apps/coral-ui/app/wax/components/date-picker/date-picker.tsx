import { getLocalTimeZone } from '@internationalized/date'
import { useDateFormatter } from 'react-aria'
import {
  Button as AriaButton,
  DatePicker as AriaDatePicker,
  DatePickerProps as AriaDatePickerProps,
  DateValue,
  Group,
} from 'react-aria-components'

import { Button } from '@/wax/components'
import { Icon } from '@/wax/components/icon'

import { Calendar } from './calendar'
import { Popover } from './popover'

export interface DatePickerProps<T extends DateValue> extends AriaDatePickerProps<T> {
  className?: string
  label?: string
  placeholder?: string
}

export function DatePicker<T extends DateValue>({
  className,
  label,
  placeholder = 'Select date',
  ...props
}: DatePickerProps<T>) {
  const formatter = useDateFormatter({ day: 'numeric', month: 'short' })

  return (
    <AriaDatePicker aria-label={label ?? placeholder} {...props} className={className}>
      {({ state }) => {
        const dateValue = state.value
        const formattedDate = dateValue
          ? formatter.format(dateValue.toDate(getLocalTimeZone()))
          : placeholder

        return (
          <>
            <Group>
              <Button.Container as={AriaButton} variant="secondary">
                <Icon color="tertiary" name="Calendar" />
                <Button.Text>{formattedDate}</Button.Text>
              </Button.Container>
            </Group>
            <Popover>
              <Calendar label={label} />
            </Popover>
          </>
        )
      }}
    </AriaDatePicker>
  )
}
