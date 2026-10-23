import type { CallOutcome } from '@/lib/activity-summary'
import type { IconName } from '@/wax/components/icon'

/** `spoken` follows a count; `spokenOne` replaces it after a count of one. */
export const outcomes: Record<
  CallOutcome,
  { label: string; icon: IconName; spoken: string; spokenOne?: string }
> = {
  answered: { label: 'Answered', icon: 'CircleCheck', spoken: 'answered' },
  blocked: { label: 'Blocked by a rule', icon: 'ShieldBan', spoken: 'blocked' },
  notAllowed: { label: 'Not allowed', icon: 'UserRoundX', spoken: 'not allowed' },
  needsSetup: {
    label: 'Needs setup',
    icon: 'Wrench',
    spoken: 'need setup',
    spokenOne: 'needs setup',
  },
  failed: { label: 'Failed', icon: 'CircleX', spoken: 'failed' },
  canceled: { label: 'Canceled', icon: 'CircleSlash', spoken: 'canceled' },
}

export const number = (value: number) => value.toLocaleString('en-US')

export const counted = (outcome: CallOutcome, value: number) => {
  const { spoken, spokenOne = spoken } = outcomes[outcome]
  return `${number(value)} ${value === 1 ? spokenOne : spoken}`
}

/** A share of a whole. A few calls never round away to 0% or 100%. */
export function share(part: number, whole: number) {
  if (whole === 0) return '0%'
  const percent = (part / whole) * 100
  if (percent > 0 && percent < 1) return percent < 0.1 ? '<0.1%' : `${percent.toFixed(1)}%`
  if (percent > 99 && percent < 100) return percent > 99.9 ? '>99.9%' : `${percent.toFixed(1)}%`
  return `${Math.round(percent)}%`
}

/** Each unit is chosen from the rounded value, so 59.6 s reads "1 min", not "60 s". */
export function duration(seconds: number) {
  const s = Math.round(seconds)
  if (s < 60) return `${s} s`
  const min = Math.round(seconds / 60)
  if (min < 60) return `${min} min`
  const h = Number((seconds / 3600).toPrecision(2))
  if (h < 24) return `${h} h`
  return `${Number((seconds / 86_400).toPrecision(2))} d`
}

/** The parts that are not empty, joined into one line. */
export const joined = (parts: readonly (string | false)[]) => parts.filter(Boolean).join(' · ')
