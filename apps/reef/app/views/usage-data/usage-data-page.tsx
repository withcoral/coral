import { CatalogToolbar } from '@/components/catalog'
import { Banner, Button, Card, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { Switch } from '@/wax/components/inputs/switch'
import { PageLayout } from '@/wax/components/page-layout'
import { Tooltip } from '@/wax/components/tooltip'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { SavedAt } from '@/views/management/saved-at'
import { blockReason, contents, docsUrl, place } from './copy'
import type { UsageDataView } from './types'
import * as styles from './usage-data.css'

/** Each report scrolls inside its card, so both fit on one screen. */
const REPORT_LINES = 20

/** The deployment's usage data setting and the exact reports. Only an organization owner opens it. */
export function UsageDataPage({
  view: v,
  error,
  onChoose,
}: {
  view: UsageDataView
  /** Why Coral did not apply the owner's last change. */
  error?: string
  onChoose: (share: boolean) => void
}) {
  // A change would not take effect while a block holds, so the switch shows off and takes none.
  const blocked = v.block && blockReason(v.block, v.kind)
  const toggle = (
    <Switch
      aria-label="Share anonymous usage data"
      checked={v.choice === 'on' && !blocked}
      disabled={Boolean(blocked)}
      onCheckedChange={onChoose}
    />
  )
  return (
    <PageLayout breadcrumbs={[{ id: 'current', type: 'text', text: 'Usage data' }]} titled compact>
      {/* The other settings pages' title row, so the title does not move between them. */}
      <CatalogToolbar
        title="Usage data"
        titleInfo={<Button.ExternalLink href={docsUrl}>Read the docs</Button.ExternalLink>}
      />
      {error && <Banner variant="error">{error}</Banner>}
      <Card.Card
        title="Share anonymous usage data"
        description={`${contents} This applies to the whole organization.`}
        action={
          blocked ? (
            // The disabled switch takes no focus, so its wrapper takes it and shows the reason.
            <Tooltip content={blocked}>
              <span className={styles.blockedSwitch} tabIndex={0}>
                {toggle}
                <span className={visuallyHidden}>{blocked}</span>
              </span>
            </Tooltip>
          ) : (
            toggle
          )
        }
      />
      {v.choice === 'on' ? (
        <Card.Card
          title="Today so far"
          description={
            v.block
              ? `A preview. Coral does not send it from ${place(v.kind)}.`
              : 'A draft: it changes until the day ends (UTC), then goes out once.'
          }
        >
          <div className={styles.report}>
            <CodeBlock value={v.next} format="json" framed maxLines={REPORT_LINES} />
          </div>
        </Card.Card>
      ) : (
        <Card.Card
          title="Today so far"
          description="Coral sends no usage data while sharing is off."
        />
      )}
      <Card.Card
        title="What Coral sent last"
        description={v.lastSent ? undefined : 'Coral has not sent a report.'}
      >
        {v.lastSent && (
          <div className={styles.report}>
            <Typography.Body as="p" variant="tertiary">
              Sent <SavedAt at={v.lastSent.at} />.
            </Typography.Body>
            <CodeBlock value={v.lastSent.json} format="json" framed maxLines={REPORT_LINES} />
          </div>
        )}
      </Card.Card>
    </PageLayout>
  )
}
