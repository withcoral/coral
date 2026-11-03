import { useState } from 'react'
import { useFetcher } from 'react-router'

import { Button, Inputs } from '@/wax/components'
import { Card } from '@/wax/components/card/card'
import { ErrorMessage } from '@/wax/components/error-message/error-message'
import { Logo } from '@/wax/components/logo'
import {
  PROVIDER_ICON_ACCEPT,
  PROVIDER_ICON_MAX_BYTES,
  PROVIDER_ICON_RULE,
} from '@/lib/provider-input-limits'
import { ActionNotice, Fields } from '@/views/management/forms'
import type { ActionResult } from '@/views/management/types'
import type { FormContext, ProviderSettingsView } from './types'
import * as styles from './providers.css'

/**
 * The logo that stands for a provider, and an owner's upload or removal of
 * their own. The preview shows the current logo: an uploaded one, a recipe's,
 * or the lettermark.
 */
export function IconCard({
  provider,
  form,
}: {
  provider: ProviderSettingsView
  form: FormContext
}) {
  const removal = useFetcher<ActionResult>()
  const removing = removal.state !== 'idle'
  return (
    <section aria-label="Icon">
      <Card title="Icon" description={PROVIDER_ICON_RULE}>
        <div className={styles.iconRow}>
          <Logo name={provider.displayName} icons={provider.icons} size="40" />
          {/* A new etag follows each change, which remounts the form and clears the file. */}
          <UploadForm key={provider.etag} provider={provider} form={form} disabled={removing} />
          {provider.uploadedIcon && (
            <removal.Form method="post">
              <Fields form={form} intent="deleteProviderIcon" etag={provider.etag} />
              <input type="hidden" name="provider" value={provider.id} />
              <Button.TextButton type="submit" variant="secondary" size="32" disabled={removing}>
                Remove icon
              </Button.TextButton>
            </removal.Form>
          )}
        </div>
        <ActionNotice result={removing ? undefined : removal.data} />
      </Card>
    </section>
  )
}

function UploadForm({
  provider,
  form,
  disabled,
}: {
  provider: ProviderSettingsView
  form: FormContext
  disabled: boolean
}) {
  const upload = useFetcher<ActionResult>()
  const uploading = upload.state !== 'idle'
  const [file, setFile] = useState<File | null>(null)
  const tooLarge = file !== null && file.size > PROVIDER_ICON_MAX_BYTES
  return (
    <upload.Form method="post" encType="multipart/form-data" className={styles.iconUpload}>
      <Fields form={form} intent="setProviderIcon" etag={provider.etag} />
      <input type="hidden" name="provider" value={provider.id} />
      <div className={styles.rowActions}>
        <Inputs.FileInput
          ariaLabel="Icon file"
          name="icon"
          accept={PROVIDER_ICON_ACCEPT}
          required
          disabled={disabled || uploading}
          invalid={tooLarge}
          onChange={setFile}
        />
        <Button.TextButton
          type="submit"
          variant="secondary"
          size="32"
          disabled={disabled || uploading || file === null || tooLarge}
        >
          Upload icon
        </Button.TextButton>
      </div>
      {tooLarge && <ErrorMessage message={`This file is too large. ${PROVIDER_ICON_RULE}`} />}
      <ActionNotice result={uploading ? undefined : upload.data} />
    </upload.Form>
  )
}
