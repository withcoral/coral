import { Link } from 'react-router'

import { Card } from '@/wax/components'
import type { CardHeaderPill } from '@/wax/components/card'

import type { CatalogEntry } from '@/lib/sources'

import { ProviderLogo } from './provider-logo'
import { sourceCatalogEntryId } from './source-catalog'
import { formatSourceName } from './source-name'

export type SourceCardEntry = CatalogEntry

export type SourceCardListInteraction =
  | {
      getEntryTo: (entry: SourceCardEntry) => string
      onPick?: never
    }
  | {
      getEntryTo?: never
      onPick: (entry: SourceCardEntry) => void
    }

export type SourceCardListProps = SourceCardListInteraction & {
  entries: SourceCardEntry[]
}

export function SourceCardList(props: SourceCardListProps) {
  return (
    <Card.List>
      {props.entries.map((entry) => {
        const sourceDisplayName = formatSourceName(entry.name)

        return (
          <Card.Item key={sourceCatalogEntryId(entry)}>
            {props.getEntryTo ? (
              <Card.Card
                as={Link}
                description={entry.description}
                headerPill={sourceOriginPill(entry)}
                icon={<ProviderLogo name={entry.name} size="small" />}
                prefetch="intent"
                preventScrollReset
                title={sourceDisplayName}
                to={props.getEntryTo(entry)}
              />
            ) : (
              <Card.Card
                as="button"
                description={entry.description}
                headerPill={sourceOriginPill(entry)}
                icon={<ProviderLogo name={entry.name} size="small" />}
                onClick={() => props.onPick(entry)}
                title={sourceDisplayName}
              />
            )}
          </Card.Item>
        )
      })}
    </Card.List>
  )
}

function sourceOriginPill(entry: SourceCardEntry): CardHeaderPill | undefined {
  if (entry.origin === 'bundled') return { label: 'Core' }
  if (entry.origin === 'imported') return { label: 'Imported' }
  return undefined
}
