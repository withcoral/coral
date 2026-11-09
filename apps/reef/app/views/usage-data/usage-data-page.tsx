import { Banner, Card, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { Switch } from '@/wax/components/inputs/switch'
import { PageLayout } from '@/wax/components/page-layout'
import { SettingsLayout } from '@/wax/components/settings-layout'
import { blockReason, contents, utc } from './copy'
import type { UsageDataView } from './types'
import * as styles from './usage-data.css'

/** The deployment's usage data setting and the exact reports. Every member can read it. */
export function UsageDataPage({
  view: v,
  onChoose,
}: {
  view: UsageDataView
  onChoose?: (share: boolean) => void
}) {
  const sends = v.choice === 'on' && !v.block
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
        {v.block && <Banner variant="info">{blockReason(v.block)}</Banner>}
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
        <Card.Card
          title="What we send"
          description={`Exactly what leaves ${place(v)}, word for word. ${
            sends
              ? 'It sends at the end of the day (UTC).'
              : v.block
                ? 'This process does not send it.'
                : 'Nothing sends while sharing is off.'
          }`}
        >
          <div className={styles.report}>
            <CodeBlock value={v.next} format="json" framed />
          </div>
        </Card.Card>
        <Card.Card
          title="What we sent last"
          description={v.lastSent ? `Sent at ${utc(v.lastSent.at)}.` : 'No report sent yet.'}
        >
          {v.lastSent && (
            <div className={styles.report}>
              <CodeBlock value={v.lastSent.json} format="json" framed />
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

function place(v: UsageDataView): string {
  return v.kind === 'desktop' ? 'your computer' : 'this server'
}
