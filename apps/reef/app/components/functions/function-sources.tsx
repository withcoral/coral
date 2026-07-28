import { TruncatedList } from '@/components/truncated-list'
import { Pill } from '@/wax/components/pill'
import { Typography } from '@/wax/components/typography'
import { pluralise } from '~/utils/pluralise'

export interface FunctionSourcesProps {
  sources: string[]
}

export function FunctionSources({ sources }: FunctionSourcesProps) {
  if (sources.length === 0) {
    return <Typography.Body variant="tertiary">—</Typography.Body>
  }

  return (
    <TruncatedList
      getKey={(source) => source}
      items={sources}
      renderItem={(source) => <Pill color="gray">{source}</Pill>}
      renderOverflowContent={(hiddenSources) =>
        hiddenSources.map((source) => (
          <Pill color="gray" key={source}>
            {source}
          </Pill>
        ))
      }
      renderOverflowTrigger={(hiddenCount) => (
        <Pill
          aria-label={`Show ${hiddenCount} more ${pluralise(hiddenCount, 'source')}`}
          as="button"
          color="gray"
        >
          +{hiddenCount}
        </Pill>
      )}
    />
  )
}
