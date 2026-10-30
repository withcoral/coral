import type { FilterQuery } from '@/utils/filter-query'
import { LINE_TERMINATOR } from '@/wax/components/code-block/lines'

import type { JsonNode } from './json-structure'

/** A string longer than this can fold to its start; in a large payload it starts folded. */
export const LONG_STRING_LENGTH = 200

export type Range = [start: number, end: number]
/** A piece of the payload Find searches: a key, a scalar or a line. A quoted key or string is
 * searched inside its quotes, so `^` and `$` mean its own start and end, as in a line. */
export type Unit = { start: number; text: string; folds: number[]; quoted?: boolean }
/** One match: the ranges of the payload its pieces matched, and the units it shows in. */
export type Match = { ranges: Range[]; units: number[] }
/** A structure's units, and its entries: each key with its scalar value, or a value alone. */
export type Structure = { units: Unit[]; entries: Range[] }

/**
 * Every place `search` matches in `units`, in order. With `structure`, it also searches each
 * entry's retained text in `source` alone, so it matches a key with its value or a quoted
 * token, and never reaches into another entry. A match that overlaps one already found is
 * not counted again. `rawOnly` counts each match of the whole text that is not one of these,
 * such as text across two entries or in punctuation alone, so Find can say that Raw shows it.
 */
export function findMatches(
  units: Unit[],
  search: FilterQuery,
  structure?: { source: string; entries: Range[] },
): { matches: Match[]; rawOnly: number } {
  if (!search.text) return { matches: [], rawOnly: 0 }
  const found = units.flatMap((unit) => {
    const text = unit.quoted ? unit.text.slice(1, -1) : unit.text
    return hitsAt(search, text, unit.start + (unit.quoted ? 1 : 0))
  })
  if (!structure) return { matches: withUnits(units, found), rawOnly: 0 }
  // Matches never overlap one another, so their spans are in order, ends too.
  const overlaps = (spans: Range[], [start, end]: Range) => {
    const after = lowerBound(spans.length, (index) => spans[index]![0] >= end)
    return after > 0 && spans[after - 1]![1] > start
  }
  const inUnits = found.map(span)
  const inEntries = structure.entries
    .flatMap(([start, end]) => hitsAt(search, structure.source.slice(start, end), start))
    .filter((ranges) => !overlaps(inUnits, span(ranges)))
  const matches = withUnits(
    units,
    [...found, ...inEntries].toSorted((a, b) => a[0]![0] - b[0]![0]),
  )
  // A Raw match counts unless it is one of these, range for range; one that only
  // overlaps them, such as `1,12` beside `12`, Raw alone shows.
  // Ranges are number pairs, so their text names them exactly.
  const shown = new Set(matches.map((match) => String(match.ranges)))
  const rawOnly = lineUnits(structure.source)
    .flatMap((line) => hitsAt(search, line.text, line.start))
    .filter((ranges) => !shown.has(String(ranges))).length
  return { matches, rawOnly }
}

/** Each key and scalar of `structure`, in offset order, with the folds that hide it. */
export function structureUnits(structure: JsonNode): Structure {
  const units: Unit[] = []
  const entries: Range[] = []
  const visit = (node: JsonNode, folds: number[]) => {
    if (node.kind === 'object') {
      const inner = [...folds, node.start]
      for (const entry of node.entries) {
        units.push({ start: entry.keyStart, text: entry.key, folds: inner, quoted: true })
        // A key with a container is an entry alone: its value holds entries of its own.
        const scalar = entry.value.kind !== 'object' && entry.value.kind !== 'array'
        entries.push([entry.keyStart, scalar ? entry.value.end : entry.keyStart + entry.key.length])
        if (scalar) scalarUnit(entry.value, inner)
        else visit(entry.value, inner)
      }
    } else if (node.kind === 'array') {
      for (const item of node.items) {
        if (item.kind === 'object' || item.kind === 'array') visit(item, [...folds, node.start])
        else {
          entries.push([item.start, item.end])
          scalarUnit(item, [...folds, node.start])
        }
      }
    } else {
      entries.push([node.start, node.end])
      scalarUnit(node, folds)
    }
  }
  const scalarUnit = (node: JsonNode, folds: number[]) =>
    units.push({
      start: node.start,
      text: 'raw' in node ? node.raw : '',
      folds:
        node.kind === 'string' && node.raw.length > LONG_STRING_LENGTH
          ? [...folds, node.start]
          : folds,
      quoted: node.kind === 'string',
    })
  visit(structure, [])
  return { units, entries }
}

/** Each line, at its exact offset, as `sourceLines` counts them. */
export function lineUnits(value: string): Unit[] {
  let start = 0
  const units: Unit[] = []
  for (const terminator of value.matchAll(LINE_TERMINATOR)) {
    units.push({ start, text: value.slice(start, terminator.index), folds: [] })
    start = terminator.index + terminator[0].length
  }
  units.push({ start, text: value.slice(start), folds: [] })
  return units
}

/** The first index below `length` where `after` holds, which it holds for every index past. */
export function lowerBound(length: number, after: (index: number) => boolean) {
  let low = 0
  let high = length
  while (low < high) {
    const middle = (low + high) >> 1
    if (after(middle)) high = middle
    else low = middle + 1
  }
  return low
}

/** Each match of `search` in `text`, its ranges moved to start at `offset`. */
const hitsAt = (search: FilterQuery, text: string, offset: number) =>
  search
    .hits(text)
    .filter((ranges) => ranges.length)
    .map((ranges) => ranges.map(([a, b]): Range => [a + offset, b + offset]))

const span = (ranges: Range[]): Range => [ranges[0]![0], ranges.at(-1)![1]]

/** Each match with the units it shows in. One that shows in none, Find cannot show here. */
const withUnits = (units: Unit[], found: Range[][]) =>
  found.flatMap((ranges) => {
    const touched = touching(units, ranges)
    return touched.length ? [{ ranges, units: touched }] : []
  })

/** The indexes of the units, in offset order, that `ranges` overlap. */
function touching(units: Unit[], ranges: Range[]) {
  const touched = new Set<number>()
  for (const [start, end] of ranges) {
    const after = lowerBound(units.length, (index) => units[index]!.start > start)
    for (let at = Math.max(after - 1, 0); at < units.length && units[at]!.start < end; at++)
      if (units[at]!.start + units[at]!.text.length > start) touched.add(at)
  }
  return [...touched].toSorted((a, b) => a - b)
}
