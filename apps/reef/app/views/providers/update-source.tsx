import { useId, type ReactNode } from 'react'
import { Form } from 'react-router'
import { EmptyPage } from '@/components/empty-page'
import { Button, Typography } from '@/wax/components'
import {
  SettingsAside,
  SettingsFact,
  SettingsFacts,
  SettingsWideColumns,
} from '@/wax/components/settings-layout'
import { Skeleton } from '@/wax/components/skeleton'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { Fields } from '@/views/management/forms'
import { SourceUrl, sourceKinds } from './source-facts'
import { redactSourceUrl } from './source-url'
import { ToolsBrowser } from './tools-section'
import { SourceStep } from './source-step'
import { StepFrame } from './step-frame'
import type { UpdatePageProps } from './types'
import * as styles from './providers.css'

/**
 * Refresh confirms the saved source. Replacing it is a separate, explicit entry.
 * As in Add provider, the tools panel is there from the start: it asks
 * you to read the source, then holds the list.
 */
export function UpdateSource({
  source,
  replacingSource = false,
  basePath,
  cancelPath,
  attempt,
  form,
  pending = false,
  result,
  children,
}: Pick<
  UpdatePageProps,
  | 'source'
  | 'replacingSource'
  | 'basePath'
  | 'cancelPath'
  | 'attempt'
  | 'form'
  | 'pending'
  | 'result'
> & {
  /** Notices about the provider's OAuth application. */
  children?: ReactNode
}) {
  const formId = useId()
  const hidden = (
    <>
      <input type="hidden" name="etag" value={attempt.etag} />
      <input type="hidden" name="task" value="refresh" />
    </>
  )
  const cancel = (
    <Button.InternalLink to={cancelPath} variant="secondary">
      Cancel
    </Button.InternalLink>
  )
  if (replacingSource)
    return (
      <StepFrame title="Replace source" actions={cancel}>
        {children}
        <SourceStep form={form} pending={pending} result={result} hidden={hidden} />
      </StepFrame>
    )
  const empty = pending ? (
    <div className={styles.toolsSkeleton} role="status">
      <span className={visuallyHidden}>Reading tools…</span>
      {SKELETON_ROWS.map((width, index) => (
        <Skeleton key={index} height="16px" width={width} />
      ))}
    </div>
  ) : source?.url ? (
    <EmptyPage
      iconName="RefreshCw"
      title="Read the latest tools"
      description="See what changed before you save."
      action={
        <Form id={formId} method="post" className={styles.emptyActions}>
          <Fields form={form} intent="prepare" />
          {hidden}
          <input type="hidden" name="source" value={source.kind} />
          <input type="hidden" name="sourceUrl" value={source.url} />
          {result?.error && (
            <Typography.Body as="p" role="alert">
              {result.error}
            </Typography.Body>
          )}
          <Button.TextButton type="submit" size="36">
            Read tools
          </Button.TextButton>
        </Form>
      }
    />
  ) : (
    <EmptyPage
      iconName="FileText"
      title="Replace the document"
      description="Upload a new document to read the latest tools."
      action={
        <Button.InternalLink to={`${basePath}?replaceSource=1`} variant="primary">
          Replace document
        </Button.InternalLink>
      }
    />
  )
  return (
    <StepFrame
      title="Check for updates"
      actions={
        <>
          {cancel}
          {/* Nothing to save until the source is read, as Next waits in Add provider. */}
          <Button.TextButton disabled>Save</Button.TextButton>
        </>
      }
    >
      {children}
      <RefreshLayout source={source}>
        <ToolsBrowser empty={empty} />
      </RefreshLayout>
    </StepFrame>
  )
}

const SKELETON_ROWS = ['40%', '65%', '55%', '70%', '45%', '60%', '50%']

/**
 * The tools beside the source they come from, as the provider's settings
 * show it. The same before and after the read.
 */
export function RefreshLayout({
  source,
  children,
}: {
  source?: UpdatePageProps['source']
  children: ReactNode
}) {
  // As on settings: never show or copy credentials, fragments or secret values.
  const shown = source?.url ? redactSourceUrl(source.url) : null
  return (
    <SettingsWideColumns
      aside={
        <SettingsAside title="Source">
          <SettingsFacts>
            <SettingsFact label="Type">
              {sourceKinds[source?.kind === 'mcp' ? 'mcp' : 'openapi']}
            </SettingsFact>
            <SettingsFact label="URL">
              {!source?.url ? (
                'Uploaded document'
              ) : shown ? (
                <SourceUrl url={shown.url} host={shown.host} />
              ) : (
                'Coral cannot show this URL.'
              )}
            </SettingsFact>
          </SettingsFacts>
        </SettingsAside>
      }
    >
      <div className={styles.stack}>{children}</div>
    </SettingsWideColumns>
  )
}
