import { Banner, Card, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { Switch } from '@/wax/components/inputs/switch'
import { PageLayout } from '@/wax/components/page-layout'
import { SettingsLayout } from '@/wax/components/settings-layout'
import { SavedAt } from '@/views/functions/saved-at'
import { blockReason, contents } from './copy'
import type { UsageDataView } from './types'
import * as styles from './usage-data.css'

/** Each report scrolls inside its card, so both fit on one screen. */
const REPORT_LINES = 20

/** The deployment's usage data setting and the exact reports. Every member can read it. */
export function UsageDataPage({
  view: v,
  onChoose,
}: {
  view: UsageDataView
  onChoose: (share: boolean) => void
}) {
  return (
    <PageLayout variant="wide" breadcrumbs={[{ id: 'current', type: 'text', text: 'Usage data' }]}>
      <SettingsLayout
        single
        header={
          <div className={styles.heading}>
            <Typography.HeadingLarge as="h2">Usage data</Typography.HeadingLarge>
          </div>
        }
      >
        {v.block && <Banner variant="info">{blockReason(v.block, v.kind)}</Banner>}
        <Card.Card
          title="Share anonymous usage data"
          description={description(v)}
          action={
            <Switch
              aria-label="Share anonymous usage data"
              checked={v.choice === 'on'}
              disabled={!v.canChange}
              onCheckedChange={onChoose}
            />
          }
        />
        {v.choice === 'on' ? (
          <Card.Card
            title="Today so far"
            description="A draft: it changes until the day ends (UTC), then goes out once."
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
      </SettingsLayout>
    </PageLayout>
  )
}

function description(v: UsageDataView): string {
  const who = v.canChange
    ? 'This applies to the whole organization.'
    : 'Only organization owners can change this.'
  return `${contents} ${who}`
}
