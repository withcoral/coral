import { useEffect, useState } from 'react'
import { useFetcher, useRevalidator } from 'react-router'

import { ErrorBanner } from '@/components/error-banner'
import { FunctionExplorer, type FunctionDetailsProps } from '@/components/functions'
import type { FunctionsActionData } from '@/routes/functions'
import { PageHeader } from '@/views/traces/page-header'
import { Button, Dialog, Typography } from '@/wax/components'

import * as styles from './functions-index.css'

export function FunctionsIndex({
  functions,
  loadError,
}: {
  functions: FunctionDetailsProps[]
  loadError: string | null
}) {
  const revalidator = useRevalidator()
  const [selectedName, setSelectedName] = useState<string>()
  const [deleteName, setDeleteName] = useState<string>()
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const deleteFetcher = useFetcher<FunctionsActionData>()
  const activeName = functions.some((fn) => fn.name === selectedName)
    ? selectedName
    : functions[0]?.name
  const deleting = deleteFetcher.state !== 'idle'
  const deleteError =
    deleteFetcher.data?.status === 'error' && deleteFetcher.data.name === deleteName
      ? deleteFetcher.data.message
      : null

  useEffect(() => {
    if (deleteFetcher.data?.status === 'success' && deleteFetcher.data.name === deleteName) {
      setDeleteDialogOpen(false)
    }
  }, [deleteFetcher.data, deleteName])

  if (!loadError) {
    return (
      <>
        <FunctionExplorer
          functions={functions}
          onDelete={(name) => {
            deleteFetcher.reset()
            setDeleteName(name)
            setDeleteDialogOpen(true)
          }}
          onSelect={setSelectedName}
          selectedName={activeName}
        />
        <Dialog.Root
          onOpenChange={(open) => {
            if (!open && !deleting) setDeleteDialogOpen(false)
          }}
          onOpenChangeComplete={(open) => {
            if (!open) setDeleteName(undefined)
          }}
          open={deleteDialogOpen}
        >
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup size="m">
              <deleteFetcher.Form method="post">
                <input name="name" type="hidden" value={deleteName ?? ''} />
                <Dialog.Title>Delete {deleteName}?</Dialog.Title>
                <Dialog.Description>
                  This deletes the function from this workspace. Queries that call it will stop
                  working.
                </Dialog.Description>
                {deleteError ? (
                  <Typography.BodySmall as="p" role="alert" variant="error">
                    {deleteError}
                  </Typography.BodySmall>
                ) : null}
                <Dialog.Actions>
                  <Button.TextButton
                    disabled={deleting}
                    onClick={() => setDeleteDialogOpen(false)}
                    type="button"
                    variant="secondary"
                  >
                    Cancel
                  </Button.TextButton>
                  <Button.TextButton disabled={deleting} type="submit" variant="destructive">
                    {deleting ? 'Deleting…' : 'Delete function'}
                  </Button.TextButton>
                </Dialog.Actions>
              </deleteFetcher.Form>
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
      </>
    )
  }

  return (
    <section aria-label="Functions" className={styles.root}>
      <PageHeader title="Functions" />
      <div className={styles.error}>
        <ErrorBanner
          message={loadError}
          onRetry={() => revalidator.revalidate()}
          title="Couldn't load functions"
        />
      </div>
    </section>
  )
}
