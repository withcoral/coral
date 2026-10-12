import { highlightCode, tagHighlighter, tags } from '@lezer/highlight'
import { parser as javascript } from '@lezer/javascript'
import { parser as json } from '@lezer/json'
import * as styles from './code-block.css'

export type Language = 'json' | 'typescript'

/** One piece of the source, with the token class that colours it, if any. */
export type Run = { text: string; className?: string }

const PARSERS = {
  json,
  typescript: javascript.configure({ dialect: 'ts' }),
}

const HIGHLIGHTER = tagHighlighter([
  { tag: tags.comment, class: styles.token.comment },
  { tag: [tags.number, tags.bool, tags.null, tags.atom], class: styles.token.constant },
  { tag: [tags.keyword, tags.self], class: styles.token.keyword },
  { tag: [tags.string, tags.regexp, tags.escape], class: styles.token.string },
  { tag: [tags.typeName, tags.className], class: styles.token.type },
  {
    tag: [tags.function(tags.variableName), tags.function(tags.propertyName)],
    class: styles.token.function,
  },
])

/** Past this size, source renders plain so an outsized program cannot stall a render. */
const MAX_HIGHLIGHT_LENGTH = 200_000

/**
 * Split source into coloured runs. Parsing is synchronous and deterministic, so the
 * server render and hydration produce the same markup. Invalid code still yields
 * every character: Lezer recovers locally and unrecognised text stays uncoloured.
 */
export function highlight(source: string, language: Language): Run[] {
  if (source.length > MAX_HIGHLIGHT_LENGTH) return [{ text: source }]
  const runs: Run[] = []
  highlightCode(
    source,
    PARSERS[language].parse(source),
    HIGHLIGHTER,
    (text, className) => runs.push(className ? { text, className } : { text }),
    () => runs.push({ text: '\n' }),
  )
  return runs
}
