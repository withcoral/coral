import { Form } from 'react-router'
import { Button, Typography } from '@/wax/components'
import { Fields } from '@/views/management/forms'
import { SourceStep } from './source-step'
import type { UpdatePageProps } from './types'
import * as styles from './providers.css'

/** Refresh confirms the saved source. Replacing it is a separate, explicit entry. */
export function UpdateSource({
  source,
  replacingSource = false,
  basePath,
  attempt,
  form,
  pending = false,
  result,
}: Pick<
  UpdatePageProps,
  'source' | 'replacingSource' | 'basePath' | 'attempt' | 'form' | 'pending' | 'result'
>) {
  const hidden = (
    <>
      <input type="hidden" name="etag" value={attempt.etag} />
      <input type="hidden" name="task" value="refresh" />
    </>
  )
  if (replacingSource)
    return <SourceStep form={form} pending={pending} result={result} hidden={hidden} />
  return (
    <div className={styles.stack}>
      {source?.url ? (
        <>
          <Typography.Body as="p">
            Read the latest operations from your saved source.
          </Typography.Body>
          <Typography.BodySmall as="p">{source.url}</Typography.BodySmall>
          <Typography.BodySmall as="p">
            Your provider stays unchanged until you review and apply the changes.
          </Typography.BodySmall>
          <Form method="post">
            <Fields form={form} intent="prepare" />
            {hidden}
            <input type="hidden" name="source" value={source.kind} />
            <input type="hidden" name="sourceUrl" value={source.url} />
            <div className={styles.stepActions}>
              <Button.TextButton type="submit" disabled={pending}>
                {pending ? 'Reading operations…' : 'Read operations'}
              </Button.TextButton>
            </div>
            {pending && (
              <Typography.BodySmall as="p" role="status">
                Reading operations. Large catalogs can take a moment.
              </Typography.BodySmall>
            )}
            {result?.error && (
              <Typography.Body as="p" role="alert">
                {result.error}
              </Typography.Body>
            )}
          </Form>
        </>
      ) : (
        <>
          <Typography.Body as="p">
            This provider uses an uploaded document. Replace the document to refresh its operations.
          </Typography.Body>
          <Button.InternalLink to={`${basePath}?replaceSource=1`}>
            Replace document
          </Button.InternalLink>
        </>
      )}
    </div>
  )
}
