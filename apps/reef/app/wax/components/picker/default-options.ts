import type { IconColor, IconName } from '@/wax/components/icon'
import type { PickerProps } from '@/wax/components/picker/picker'

export const STATUS_PICKER_OPTIONS: PickerProps['options'] = [
  { icon: { color: 'error', name: 'CircleDot' }, label: 'Open', value: 'OPEN' },
  { icon: { color: 'success', name: 'CircleCheck' }, label: 'Resolved', value: 'RESOLVED' },
  { icon: { color: 'tertiary', name: 'CircleQuestionMark' }, label: 'Unknown', value: 'UNKNOWN' },
]

export const RISK_ICONS: Record<string, IconName> = {
  Critical: 'SeverityCritical',
  Low: 'SeverityLow',
  Moderate: 'SeverityModerate',
  Severe: 'SeveritySevere',
}

export const RISK_PICKER_OPTIONS: PickerProps['options'] = [
  { icon: { color: 'inherit', name: RISK_ICONS.Severe }, label: 'Severe', value: 'Severe' },
  { icon: { color: 'inherit', name: RISK_ICONS.Moderate }, label: 'Moderate', value: 'Moderate' },
  { icon: { color: 'inherit', name: RISK_ICONS.Low }, label: 'Low', value: 'Low' },
]

export const DETECTION_SOURCE_PICKER_OPTIONS: PickerProps['options'] = [
  { label: 'Internal source', value: 'INTERNAL' },
  { label: 'External source', value: 'EXTERNAL' },
  { label: 'Unknown source', value: 'UNKNOWN' },
]

/**
 * Build severity picker options from organization settings.
 * Severity levels are configurable per organization.
 * Always includes an "Unknown" option for empty/missing values.
 */
export interface SeverityPickerSettings {
  severity_levels: SeverityPickerLevel[]
}

export interface SeverityPickerLevel {
  label: string
  order: number
}

export function buildSeverityPickerOptions(
  settings: SeverityPickerSettings,
): PickerProps['options'] {
  const options: PickerProps['options'] = settings.severity_levels.map((level) => {
    const color = getSeverityColor(level.order)
    return {
      icon: { color, name: 'Flag' },
      label: level.label,
      value: level.label,
    } satisfies PickerProps['options'][number]
  })

  // Add Unknown option for empty/missing severity values
  options.push({
    icon: { color: 'tertiary', name: 'Flag' },
    label: 'Unknown',
    value: UNKNOWN_VALUE,
  })

  return options
}

/** Sentinel value for unknown/empty picker values */
export const UNKNOWN_VALUE = '__UNKNOWN__'

/** Normalize a picker value, treating empty strings as unknown */
export function normalizePickerValue(value: string): string {
  return value === '' ? UNKNOWN_VALUE : value
}

/**
 * Get severity icon color based on severity order.
 * Matches the color logic used in severity-chip.tsx.
 */
function getSeverityColor(order: number): IconColor {
  if (order === 0) return 'error'
  if (order === 1) return 'orange'
  if (order >= 2) return 'warning'
  return 'tertiary'
}
