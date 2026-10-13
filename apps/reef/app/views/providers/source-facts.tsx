import { Button, Typography } from '@/wax/components'
import { Tooltip } from '@/wax/components/tooltip'
import * as styles from './providers.css'

/** A provider source's kind, as settings and a refresh name it. */
export const sourceKinds = { openapi: 'OpenAPI document', mcp: 'MCP server' } as const

/**
 * The host reads at a glance; the whole URL stays on one line, shortened when
 * it does not fit, with its full text in a tooltip and a copy button.
 */
export function SourceUrl({ url, host }: { url: string; host: string }) {
  return (
    <span className={styles.settingsUrl}>
      <Typography.BodySmall as="span">{host}</Typography.BodySmall>
      <span className={styles.settingsUrlRow}>
        <Tooltip content={url} showOnlyWhenTruncated maxWidth={520}>
          <span className={styles.settingsUrlText}>
            <Typography.BodySmall as="span" variant="tertiary">
              {url}
            </Typography.BodySmall>
          </span>
        </Tooltip>
        <Button.CopyButton textToCopy={url} ariaLabel="Copy source URL" size="22" variant="bare" />
      </span>
    </span>
  )
}
