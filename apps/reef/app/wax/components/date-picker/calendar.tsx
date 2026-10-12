import classNames from 'classnames'
import { useLocale } from 'react-aria'
import {
  Button as AriaButton,
  Calendar as AriaCalendar,
  CalendarProps as AriaCalendarProps,
  CalendarCell,
  CalendarGrid,
  CalendarGridBody,
  CalendarGridHeader,
  CalendarHeaderCell,
  DateValue,
  Heading,
} from 'react-aria-components'

import { Button, Typography } from '@/wax/components'

import * as styles from './calendar.css'

export interface CalendarProps<T extends DateValue> extends Omit<
  AriaCalendarProps<T>,
  'visibleDuration'
> {
  className?: string
  label?: string
}

// Designs only have 2 letters (E.g. "Mo", "Tu"). React Aria uses `Intl`, which only provides 1, 3 or full name.
// Only some locale work nicely with only 2 letters, but not all. E.g. Brazilian (Quarta-feira, Quinta-feira, wouldn't work),
// RTL languages might break too.
// Instead, we keep a list of popular languages we know are safe to slice.
const SAFE_2LETTER_LOCALES = ['en', 'fr', 'de', 'nl', 'it']

export function Calendar<T extends DateValue>({ className, label, ...props }: CalendarProps<T>) {
  const { locale } = useLocale()
  const isSafeToSlice = SAFE_2LETTER_LOCALES.some(
    (lang) => locale === lang || locale.startsWith(`${lang}-`),
  )

  return (
    <AriaCalendar {...props} className={classNames(styles.calendar, className)}>
      {label && (
        <div className={styles.label}>
          <Typography.BodySmallStrong variant="tertiary">{label}</Typography.BodySmallStrong>
        </div>
      )}
      <header className={styles.header}>
        <Button.Container as={AriaButton} size="22" slot="previous" variant="bare">
          <Button.Icon name="ChevronLeft" />
        </Button.Container>
        <Heading className={styles.heading} />
        <Button.Container as={AriaButton} size="22" slot="next" variant="bare">
          <Button.Icon name="ChevronRight" />
        </Button.Container>
      </header>
      <CalendarGrid className={styles.calendarGrid} weekdayStyle="short">
        <CalendarGridHeader>
          {(day) => (
            <CalendarHeaderCell>{isSafeToSlice ? day.slice(0, 2) : day}</CalendarHeaderCell>
          )}
        </CalendarGridHeader>
        <CalendarGridBody>
          {(date) => <CalendarCell className={styles.calendarCell} date={date} />}
        </CalendarGridBody>
      </CalendarGrid>
    </AriaCalendar>
  )
}
