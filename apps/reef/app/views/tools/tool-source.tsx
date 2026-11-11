import { Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { Section } from '@/views/management/layout'
import type { ToolDiagnosticView, ToolSourceView } from './types'
import * as styles from './tools.css'

// The drawer caps the source; the rest scrolls in place.
const SOURCE_LINES = 20

const categories: Record<ToolDiagnosticView['category'], string> = {
  error: 'Error',
  warning: 'Warning',
  suggestion: 'Suggestion',
  message: 'Message',
}

function location(d: ToolDiagnosticView) {
  if (d.line === null) return 'Whole source'
  return d.column === null ? `Line ${d.line}` : `Line ${d.line}, column ${d.column}`
}

/**
 * The diagnostics Coral kept of a source, each on two lines: its message, then
 * its kind and where it is. They are advisory: a source with diagnostics still
 * runs. With none, it says `none`.
 */
export function Diagnostics({
  diagnostics,
  truncated,
  none,
}: {
  diagnostics: ToolDiagnosticView[]
  /** Coral kept only the first diagnostics and dropped the rest. */
  truncated: boolean
  none: string
}) {
  return (
    <>
      {diagnostics.length === 0 ? (
        <Typography.Body as="p">{none}</Typography.Body>
      ) : (
        <ul aria-label="Diagnostics" className={styles.lines}>
          {diagnostics.map((d, index) => (
            <li key={index} className={styles.line}>
              <span className={styles.lineMain}>
                <Typography.BodySmall as="span">{`TS${d.code}: ${d.message}`}</Typography.BodySmall>
                <Typography.BodySmall as="span" variant="secondary">
                  {`${categories[d.category]} · ${location(d)}`}
                </Typography.BodySmall>
              </span>
            </li>
          ))}
        </ul>
      )}
      {truncated && (
        <Typography.BodySmall as="p" variant="secondary">
          {`Showing the first ${diagnostics.length.toLocaleString('en-US')} diagnostics only.`}
        </Typography.BodySmall>
      )}
    </>
  )
}

/**
 * What Coral keeps of a private or a shared tool's source: its declaration,
 * the source, and the diagnostics of the save or the publish that made it.
 * A source with no diagnostics shows no section for them.
 */
export function ToolSource({
  source,
  diagnosticsNote,
}: {
  source: ToolSourceView
  /** Says which save or publish the diagnostics are from. */
  diagnosticsNote: string
}) {
  return (
    <>
      <Section title="Declaration">
        <CodeBlock value={source.declaration} format="typescript" framed />
      </Section>
      <Section title="Source">
        <CodeBlock value={source.source} format="typescript" framed maxLines={SOURCE_LINES} />
      </Section>
      {source.diagnostics.length > 0 && (
        <Section title="Diagnostics" description={diagnosticsNote}>
          <Diagnostics
            diagnostics={source.diagnostics}
            truncated={source.diagnosticsTruncated}
            none=""
          />
        </Section>
      )}
    </>
  )
}
