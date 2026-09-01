import { Form } from 'react-router'

import { routePath } from '@/routing/routemap'
import { Button, Inputs, Typography } from '@/wax/components'

import * as styles from './create-first-workspace.css'

/**
 * The route reads the submission state, so both sides address the same fetcher.
 */
export const CREATE_FIRST_WORKSPACE_FETCHER_KEY = 'create-first-workspace'

export interface CreateFirstWorkspaceProps {
  error?: string
  pending?: boolean
}

/**
 * Full-page fallback for a caller that no workspace lists as a member. Any human
 * may create a workspace, and creation makes them its owner, so offer that here
 * instead of the app shell that gates the usual creation dialog.
 */
export function CreateFirstWorkspace({ error, pending = false }: CreateFirstWorkspaceProps) {
  return (
    <main className={styles.page}>
      <section aria-labelledby="create-first-workspace-title" className={styles.card}>
        <div className={styles.intro}>
          <Typography.HeadingMedium as="h1" id="create-first-workspace-title">
            Create your first workspace
          </Typography.HeadingMedium>
          <Typography.Body variant="tertiary">
            No workspace lists you as a member yet. Create one to get started.
          </Typography.Body>
        </div>

        <Form
          action={routePath('workspaces')}
          className={styles.form}
          fetcherKey={CREATE_FIRST_WORKSPACE_FETCHER_KEY}
          method="post"
          navigate={false}
        >
          <input name="intent" type="hidden" value="create" />
          <div className={styles.field}>
            <Typography.BodySmallStrong as="label" htmlFor="create-first-workspace-name">
              Workspace name
            </Typography.BodySmallStrong>
            <Inputs.TextInput
              autoFocus
              id="create-first-workspace-name"
              invalid={Boolean(error)}
              name="name"
              placeholder="engineering"
            />
            {error ? (
              <Typography.BodySmall as="p" role="alert" variant="error">
                {error}
              </Typography.BodySmall>
            ) : (
              <Typography.BodySmall as="p" variant="tertiary">
                Use letters, numbers, and hyphens.
              </Typography.BodySmall>
            )}
          </div>
          <Button.TextButton disabled={pending} fullWidth type="submit">
            {pending ? 'Creating…' : 'Create workspace'}
          </Button.TextButton>
        </Form>
      </section>
    </main>
  )
}
