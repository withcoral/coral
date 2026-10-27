import { Typography } from '@/wax/components'
import { NumberPopIn } from '@/wax/components/number-pop-in'

const units = ['B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB'] as const

/**
 * Round only the display; retain exact bytes for accounting and the tooltip.
 * One element renders every state, so a live update pops the new value in.
 * A partial total sums only finished work and changes when running work ends.
 */
export function ContextImpact({ bytes, partial = false }: { bytes?: string; partial?: boolean }) {
  const impact = bytes === undefined ? undefined : describeContextImpact(BigInt(bytes), partial)
  return (
    <Typography.CodeSmallInline
      variant={impact?.variant ?? 'tertiary'}
      aria-label={impact?.meaning ?? 'Context impact unavailable'}
      title={impact?.title ?? 'A complete measurement is unavailable.'}
    >
      <NumberPopIn value={impact?.label ?? '—'} />
    </Typography.CodeSmallInline>
  )
}

/** Rounded label, spoken meaning, one-word change, exact title and tone for net context bytes. */
export function describeContextImpact(value: bigint, partial: boolean) {
  const magnitude = value < 0n ? -value : value
  // Round before selecting the unit so 999.5 kB becomes 1 MB, not 1,000 kB.
  let size = Number(Number(magnitude).toPrecision(3))
  let unit = 0
  while (size >= 1000 && unit < units.length - 1) {
    size /= 1000
    unit++
  }
  const formatted = `${size.toLocaleString('en-US', { maximumSignificantDigits: 3 })} ${units[unit]}`
  const direction = value < 0n ? 'saved' : 'added'
  const exact = `${magnitude.toLocaleString('en-US')} ${magnitude === 1n ? 'byte' : 'bytes'}${value === 0n ? '' : ` ${direction}`}`
  return {
    label: `${value < 0n ? '−' : value > 0n ? '+' : ''}${formatted}`,
    change: value < 0n ? 'Saved' : value > 0n ? 'Added' : 'No change',
    meaning: `${value === 0n ? 'No net context change' : `${formatted} ${direction}`}${partial ? ' so far' : ''}`,
    title: `${partial ? 'Some work is still running. This total changes when it ends. ' : ''}${exact}. Display values are rounded. Compare retained output with tool results. Parent totals use exact byte counts. Totals include all pages. This measures payload bytes, not tokens.`,
    variant:
      value < 0n ? ('success' as const) : value > 0n ? ('error' as const) : ('tertiary' as const),
  }
}
