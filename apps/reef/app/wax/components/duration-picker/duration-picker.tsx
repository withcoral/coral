import { Popover } from '@base-ui/react/popover'
import { useEffect, useState } from 'react'

import { Button, Inputs, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'

import * as styles from './duration-picker.css'

/**
 * Duration picker with day/hour/minute precision.
 * Note: Seconds are truncated - a value of 90061s (1d 1h 1m 1s) displays as 1d 1h 1m.
 */
export interface DurationPickerProps {
  icon: IconName
  label: string
  onChange?: (seconds: null | number) => void
  onClose?: (seconds: number) => void
  value?: null | number
}

const SECONDS_PER_MINUTE = 60
const SECONDS_PER_HOUR = 3600
const SECONDS_PER_DAY = 86400

export function DurationPicker({ icon, label, onChange, onClose, value }: DurationPickerProps) {
  const [localFields, setLocalFields] = useState(() => secondsToFields(value ?? null))

  const handleOpenChange = (open: boolean) => {
    if (!open) {
      onClose?.(fieldsToSeconds(localFields))
    }
  }

  useEffect(() => {
    setLocalFields(secondsToFields(value ?? null))
  }, [value])

  const handleDaysChange = (newDays: null | number) => {
    const newFields = { ...localFields, days: Math.max(0, newDays ?? 0) }
    setLocalFields(newFields)
    onChange?.(fieldsToSeconds(newFields))
  }

  const handleHoursChange = (newHours: null | number) => {
    const newFields = { ...localFields, hours: Math.max(0, newHours ?? 0) }
    setLocalFields(newFields)
    onChange?.(fieldsToSeconds(newFields))
  }

  const handleMinutesChange = (newMinutes: null | number) => {
    const newFields = { ...localFields, minutes: Math.max(0, newMinutes ?? 0) }
    setLocalFields(newFields)
    onChange?.(fieldsToSeconds(newFields))
  }

  const handleHoursBlur = () => {
    if (localFields.hours > 23) {
      const extraDays = Math.floor(localFields.hours / 24)
      const newFields = {
        ...localFields,
        days: localFields.days + extraDays,
        hours: localFields.hours % 24,
      }
      setLocalFields(newFields)
      onChange?.(fieldsToSeconds(newFields))
    }
  }

  const handleMinutesBlur = () => {
    if (localFields.minutes > 59) {
      const extraHours = Math.floor(localFields.minutes / 60)
      const normalizedMinutes = localFields.minutes % 60
      const totalHours = localFields.hours + extraHours
      const newFields = {
        days: localFields.days + (totalHours > 23 ? Math.floor(totalHours / 24) : 0),
        hours: totalHours > 23 ? totalHours % 24 : totalHours,
        minutes: normalizedMinutes,
      }
      setLocalFields(newFields)
      onChange?.(fieldsToSeconds(newFields))
    }
  }

  return (
    <Popover.Root onOpenChange={handleOpenChange}>
      <Popover.Trigger render={<Button.Container variant="secondary" />}>
        <Icon color="tertiary" name={icon} />
        <Button.Text>{formatDuration(localFields) ?? label}</Button.Text>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner align="start" side="bottom" sideOffset={4}>
          <Popover.Popup className={styles.popup}>
            <div className={styles.label}>
              <Typography.BodySmallStrong variant="tertiary">{label}</Typography.BodySmallStrong>
            </div>
            <div className={styles.fields}>
              <Inputs.NumberInput
                className={styles.input}
                min={0}
                onChange={handleDaysChange}
                placeholder="0"
                value={localFields.days}
              />
              <Typography.Body variant="tertiary">d</Typography.Body>
              <Inputs.NumberInput
                className={styles.input}
                min={0}
                onBlur={handleHoursBlur}
                onChange={handleHoursChange}
                placeholder="0"
                value={localFields.hours}
              />
              <Typography.Body variant="tertiary">h</Typography.Body>
              <Inputs.NumberInput
                className={styles.input}
                min={0}
                onBlur={handleMinutesBlur}
                onChange={handleMinutesChange}
                placeholder="0"
                value={localFields.minutes}
              />
              <Typography.Body variant="tertiary">m</Typography.Body>
            </div>
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  )
}

function fieldsToSeconds({
  days,
  hours,
  minutes,
}: {
  days: number
  hours: number
  minutes: number
}): number {
  return days * SECONDS_PER_DAY + hours * SECONDS_PER_HOUR + minutes * SECONDS_PER_MINUTE
}

function formatDuration(fields: { days: number; hours: number; minutes: number }): null | string {
  const parts: string[] = []
  if (fields.days > 0) parts.push(`${fields.days}d`)
  if (fields.hours > 0) parts.push(`${fields.hours}h`)
  if (fields.minutes > 0) parts.push(`${fields.minutes}m`)
  return parts.length > 0 ? parts.join(' ') : null
}

function secondsToFields(seconds: null | number): { days: number; hours: number; minutes: number } {
  if (seconds === null || seconds === 0) {
    return { days: 0, hours: 0, minutes: 0 }
  }
  const days = Math.floor(seconds / SECONDS_PER_DAY)
  const hours = Math.floor((seconds % SECONDS_PER_DAY) / SECONDS_PER_HOUR)
  const minutes = Math.floor((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE)
  return { days, hours, minutes }
}
