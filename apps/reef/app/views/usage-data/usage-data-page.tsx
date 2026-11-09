import { Banner, Card, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { Switch } from '@/wax/components/inputs/switch'
import { PageLayout } from '@/wax/components/page-layout'
import { SettingsLayout } from '@/wax/components/settings-layout'
import { SavedAt } from '@/views/functions/saved-at'
import { blockReason, contents, place } from './copy'
import type { UsageDataView } from './types'
import * as styles from './usage-data.css'

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
        <Card.Card title="What Coral sends" description={nextDescription(v)}>
          <div className={styles.report}>
            {/* Text, not JSON: the JSON format re-indents the value, and this shows it byte for byte. */}
            <CodeBlock value={v.next} framed wrap />
          </div>
        </Card.Card>
        <Card.Card
          title="What Coral sent last"
          description={v.lastSent ? undefined : 'Coral has not sent a report.'}
        >
          {v.lastSent && (
            <div className={styles.report}>
              <Typography.Body as="p" variant="tertiary">
                Sent <SavedAt at={v.lastSent.at} />.
              </Typography.Body>
              <CodeBlock value={v.lastSent.json} framed wrap />
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

function nextDescription(v: UsageDataView): string {
  const report = `The next report, exactly as it leaves ${place(v.kind)}.`
  // The banner already says why a blocked process sends nothing.
  return v.choice === 'off' && !v.block
    ? `${report} Coral does not send it while sharing is off.`
    : report
}
