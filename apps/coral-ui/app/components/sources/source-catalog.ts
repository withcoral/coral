import { SOURCE_CATEGORY_ORDER, getCategoryForSource } from '@/lib/source-categories'
import type { CatalogEntry } from '@/lib/sources'

export type SourceCatalogEntry = CatalogEntry

export interface SourceCatalogSection {
  count: number
  entries: SourceCatalogEntry[]
  key: string
  title: string
}

export interface SourceCatalogView {
  allEntries: SourceCatalogEntry[]
  connected: SourceCatalogEntry[]
  filtered: SourceCatalogEntry[]
  sections: SourceCatalogSection[]
}

export function sourceCatalogEntryId(entry: SourceCatalogEntry) {
  return `${entry.origin}:${entry.name}`
}

export function buildSourceCatalogView(
  entries: SourceCatalogEntry[],
  search: string,
): SourceCatalogView {
  const allEntries = entries.toSorted((a, b) => a.name.localeCompare(b.name))
  const filtered = filterSourceCatalogEntries(allEntries, search)
  const connected = filtered.filter((entry) => entry.installed)
  const sections = groupSourceCatalogSections(filtered)

  return { allEntries, connected, filtered, sections }
}

function filterSourceCatalogEntries(entries: SourceCatalogEntry[], search: string) {
  const q = search.trim().toLowerCase()
  if (!q) return entries

  return entries.filter(
    (entry) => entry.name.toLowerCase().includes(q) || entry.description.toLowerCase().includes(q),
  )
}

function groupSourceCatalogSections(entries: SourceCatalogEntry[]): SourceCatalogSection[] {
  const grouped = new Map<string, SourceCatalogEntry[]>()

  for (const entry of entries) {
    if (entry.installed) continue

    const category = getCategoryForSource(entry.name)
    const group = grouped.get(category)
    if (group) {
      group.push(entry)
    } else {
      grouped.set(category, [entry])
    }
  }

  const ordered = SOURCE_CATEGORY_ORDER.map((category) => ({
    count: grouped.get(category.key)?.length ?? 0,
    entries: grouped.get(category.key) ?? [],
    key: category.key,
    title: category.label,
  })).filter((category) => category.entries.length > 0)

  const other = grouped.get('other')
  if (other?.length) {
    ordered.push({ count: other.length, entries: other, key: 'other', title: 'Other' })
  }

  return ordered
}
