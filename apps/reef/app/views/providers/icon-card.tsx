import { useId, useRef, useState } from 'react'
import { useFetcher, type FetcherWithComponents } from 'react-router'

import { Button, Inputs } from '@/wax/components'
import { Card } from '@/wax/components/card/card'
import { Logo } from '@/wax/components/logo'
import {
  PROVIDER_ICON_ACCEPT,
  PROVIDER_ICON_MAX_BYTES,
  PROVIDER_ICON_RULE,
} from '@/lib/provider-input-limits'
import { ActionNotice, Fields } from '@/views/management/forms'
import type { ActionResult } from '@/views/management/types'
import { FieldError } from './fields'
import type { FormContext, ProviderSettingsView } from './types'
import * as styles from './providers.css'

/**
 * The logo that stands for a provider, and an owner's upload or removal of
 * their own. The preview shows the current logo: an uploaded one, a recipe's,
 * or the lettermark. Choosing a file within the limits uploads it at once.
 */
export function IconCard({
  provider,
  form,
}: {
  provider: ProviderSettingsView
  form: FormContext
}) {
  // Both changes send the same etag, so only one runs at a time.
  const upload = useFetcher<ActionResult>()
  const removal = useFetcher<ActionResult>()
  const uploading = upload.state !== 'idle'
  const removing = removal.state !== 'idle'
  return (
    <section aria-label="Icon">
      <Card title="Icon" description={PROVIDER_ICON_RULE}>
        <div className={styles.iconRow}>
          <Logo name={provider.displayName} icons={provider.icons} size="40" />
          {/* A new etag follows each change, which remounts the form and clears its error. */}
          <UploadForm
            key={provider.etag}
            provider={provider}
            form={form}
            upload={upload}
            disabled={removing}
          />
          {provider.uploadedIcon && (
            <removal.Form method="post">
              <Fields form={form} intent="deleteProviderIcon" etag={provider.etag} />
              <input type="hidden" name="provider" value={provider.id} />
              <Button.TextButton
                type="submit"
                variant="secondary"
                size="32"
                busy={removing}
                disabled={uploading}
              >
                Remove icon
              </Button.TextButton>
            </removal.Form>
          )}
        </div>
        <ActionNotice result={removing ? undefined : removal.data} />
        <ActionNotice result={uploading ? undefined : upload.data} />
      </Card>
    </section>
  )
}

function UploadForm({
  provider,
  form,
  upload,
  disabled,
}: {
  provider: ProviderSettingsView
  form: FormContext
  upload: FetcherWithComponents<ActionResult>
  disabled: boolean
}) {
  const picker = useRef<HTMLInputElement>(null)
  const [tooLarge, setTooLarge] = useState(false)
  const errorId = useId()
  const choose = (file: File | null) => {
    const input = picker.current
    if (!file || !input?.form) return
    setTooLarge(file.size > PROVIDER_ICON_MAX_BYTES)
    if (file.size <= PROVIDER_ICON_MAX_BYTES) {
      upload.submit(new FormData(input.form), {
        method: 'post',
        encType: 'multipart/form-data',
      })
    }
    // A browser reports no change for the same file, so clear it to allow a retry.
    input.value = ''
  }
  return (
    <upload.Form method="post" encType="multipart/form-data" className={styles.iconUpload}>
      <Fields form={form} intent="setProviderIcon" etag={provider.etag} />
      <input type="hidden" name="provider" value={provider.id} />
      <Inputs.FileInput
        ref={picker}
        ariaLabel="Icon file"
        buttonLabel="Upload icon"
        name="icon"
        accept={PROVIDER_ICON_ACCEPT}
        busy={upload.state !== 'idle'}
        disabled={disabled}
        invalid={tooLarge}
        ariaDescribedBy={tooLarge ? errorId : undefined}
        onChange={choose}
      />
      {tooLarge && (
        <div className={styles.iconError}>
          <FieldError id={errorId} message={`This file is too large. ${PROVIDER_ICON_RULE}`} />
        </div>
      )}
    </upload.Form>
  )
}
