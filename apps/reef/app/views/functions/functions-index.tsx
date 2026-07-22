import { useRef, useState } from 'react'
import { Form, Link, useNavigate, useNavigation, useRevalidator } from 'react-router'

import { Button, Dialog, Inputs, ScrollArea, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'

import { EmptyPage } from '@/components/empty-page'
import { ErrorBanner } from '@/components/error-banner'
import { highlightSQL } from '@/lib/sql-highlight'
import type {
  FunctionEditor,
  FunctionsActionData,
  FunctionSummary,
} from '@/routes/functions.server'
import { routePath } from '@/routing/routemap'

import * as styles from './functions-index.css'

export function FunctionsIndex({
  actionData,
  editor,
  functions,
  loadError,
  workspaceId,
}: {
  actionData: FunctionsActionData
  editor: FunctionEditor
  functions: FunctionSummary[]
  loadError: string | null
  workspaceId: string
}) {
  const navigate = useNavigate()
  const revalidator = useRevalidator()
  const functionsPath = routePath('workspaceFunctions', { workspaceId })

  return (
    <section aria-label="Coral functions" className={styles.root}>
      <div className={styles.header}>
        <div className={styles.headerText}>
          <Typography.HeadingLarge as="h1">Functions</Typography.HeadingLarge>
          <Typography.Body variant="secondary">
            Turn reusable SQL into table functions that agents and queries can call.
          </Typography.Body>
        </div>
        <Button.Container as={Link} to={`${functionsPath}?new`}>
          <Button.Icon name="Plus" />
          <Button.Text>New function</Button.Text>
        </Button.Container>
      </div>

      {loadError ? (
        <div className={styles.statusPanel}>
          <ErrorBanner
            message={loadError}
            onRetry={() => revalidator.revalidate()}
            title="Couldn't load functions"
          />
        </div>
      ) : null}

      <ScrollArea.Container className={styles.scroll} constrainWidth fillContent>
        <div className={styles.content}>
          {!loadError && functions.length === 0 ? (
            <EmptyPage
              action={
                <Button.Container as={Link} size="32" to={`${functionsPath}?new`}>
                  <Button.Icon name="Plus" />
                  <Button.Text>New function</Button.Text>
                </Button.Container>
              }
              description="Create reusable, parameterized SQL for your workspace."
              iconName="Braces"
              title="No functions yet"
            />
          ) : null}
          {functions.length > 0 ? (
            <div className={styles.list}>
              {functions.map((fn) => (
                <FunctionRow fn={fn} functionsPath={functionsPath} key={fn.name} />
              ))}
            </div>
          ) : null}
        </div>
      </ScrollArea.Container>

      {editor?.mode === 'delete' ? (
        <DeleteFunctionDialog
          actionData={actionData}
          name={editor.name}
          onClose={() => navigate(functionsPath)}
        />
      ) : editor ? (
        <FunctionEditorDialog
          actionData={actionData}
          editor={editor}
          key={`${editor.mode}:${editor.artifact.name}`}
          onClose={() => navigate(functionsPath)}
        />
      ) : null}
    </section>
  )
}

function FunctionRow({ fn, functionsPath }: { fn: FunctionSummary; functionsPath: string }) {
  const signature = `${fn.schema ? `${fn.schema}.` : ''}${fn.name}(${fn.arguments
    .map((argument) => `${argument.name}: ${argument.dataType}`)
    .join(', ')})`

  return (
    <article className={styles.functionRow}>
      <div className={styles.functionIcon}>
        <Icon color="secondary" name="Braces" size="20" />
      </div>
      <div className={styles.functionDetails}>
        <div className={styles.functionTitle}>
          <Typography.BodyStrong>{fn.name}</Typography.BodyStrong>
          <Pill color={fn.status === 'ready' ? 'green' : 'red'}>
            {fn.status === 'ready' ? 'Ready' : 'Invalid'}
          </Pill>
        </div>
        <code className={styles.signature}>{signature}</code>
        <Typography.BodySmall variant={fn.error ? 'error' : 'secondary'}>
          {fn.error || fn.description || 'No description'}
        </Typography.BodySmall>
      </div>
      <div className={styles.actions}>
        <Button.Container
          ariaLabel={`Edit ${fn.name}`}
          as={Link}
          size="32"
          to={`${functionsPath}?edit=${encodeURIComponent(fn.name)}`}
          variant="secondary"
        >
          <Button.Icon name="Pencil" />
        </Button.Container>
        <Button.Container
          ariaLabel={`Delete ${fn.name}`}
          as={Link}
          size="32"
          to={`${functionsPath}?delete=${encodeURIComponent(fn.name)}`}
          variant="bare"
        >
          <Button.Icon name="Trash2" />
        </Button.Container>
      </div>
    </article>
  )
}

function FunctionEditorDialog({
  actionData,
  editor,
  onClose,
}: {
  actionData: FunctionsActionData
  editor: Extract<FunctionEditor, { mode: 'edit' | 'new' }>
  onClose: () => void
}) {
  const navigation = useNavigation()
  const [artifact, setArtifact] = useState(editor.artifact)
  const saving = navigation.state !== 'idle' && navigation.formData?.get('_intent') === 'save'
  const error = editor.loadError || (actionData?.intent === 'save' ? actionData.message : null)

  return (
    <Dialog.Root onOpenChange={(open) => !open && onClose()} open>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="xl">
          <Dialog.Title>
            {editor.mode === 'new' ? 'New function' : `Edit ${artifact.name}`}
          </Dialog.Title>
          <Dialog.Description>
            Define the SQL namespace, description, and query body. Coral infers parameters from the
            SQL when it validates the function.
          </Dialog.Description>
          <Dialog.Close />
          <Form className={styles.form} method="post">
            <input name="_intent" type="hidden" value="save" />
            {editor.mode === 'edit' ? (
              <input name="originalName" type="hidden" value={editor.artifact.name} />
            ) : null}
            <div className={styles.fieldsRow}>
              <FunctionField label="Name">
                <Inputs.TextInput
                  autoFocus={editor.mode === 'new'}
                  name="name"
                  onChange={(name) => setArtifact((value) => ({ ...value, name }))}
                  placeholder="retrieve_pull_requests"
                  readOnly={editor.mode === 'edit'}
                  value={artifact.name}
                />
              </FunctionField>
              <FunctionField label="Schema">
                <Inputs.TextInput
                  name="schema"
                  onChange={(schema) => setArtifact((value) => ({ ...value, schema }))}
                  placeholder="github"
                  value={artifact.schema}
                />
              </FunctionField>
            </div>
            <FunctionField label="Description">
              <textarea
                aria-label="Description"
                className={styles.descriptionEditor}
                name="description"
                onChange={(event) =>
                  setArtifact((value) => ({ ...value, description: event.target.value }))
                }
                placeholder="Retrieve pull requests from a GitHub repository"
                value={artifact.description}
              />
            </FunctionField>
            <FunctionField label="SQL">
              <SqlEditor
                onChange={(sql) => setArtifact((value) => ({ ...value, sql }))}
                value={artifact.sql}
              />
            </FunctionField>
            {error ? (
              <Typography.BodySmall as="p" role="alert" variant="error">
                {error}
              </Typography.BodySmall>
            ) : null}
            <Dialog.Actions>
              <Button.TextButton
                disabled={saving}
                onClick={onClose}
                type="button"
                variant="secondary"
              >
                Cancel
              </Button.TextButton>
              <Button.TextButton disabled={saving || Boolean(editor.loadError)} type="submit">
                {saving ? 'Saving…' : editor.mode === 'new' ? 'Create function' : 'Save changes'}
              </Button.TextButton>
            </Dialog.Actions>
          </Form>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SqlEditor({ onChange, value }: { onChange: (value: string) => void; value: string }) {
  const highlight = useRef<HTMLPreElement>(null)

  return (
    <div className={styles.sqlEditorShell}>
      <pre
        aria-hidden
        className={styles.sqlHighlight}
        dangerouslySetInnerHTML={{ __html: highlightSQL(`${value}\n`) }}
        ref={highlight}
      />
      <textarea
        aria-label="SQL"
        className={styles.sqlEditor}
        name="sql"
        onChange={(event) => onChange(event.target.value)}
        onScroll={(event) => {
          if (!highlight.current) return
          highlight.current.scrollTop = event.currentTarget.scrollTop
          highlight.current.scrollLeft = event.currentTarget.scrollLeft
        }}
        placeholder={
          'select number, title, html_url\nfrom github.pulls(owner => $owner, repo => $repo)'
        }
        spellCheck={false}
        value={value}
      />
    </div>
  )
}

function DeleteFunctionDialog({
  actionData,
  name,
  onClose,
}: {
  actionData: FunctionsActionData
  name: string
  onClose: () => void
}) {
  const navigation = useNavigation()
  const deleting = navigation.state !== 'idle' && navigation.formData?.get('_intent') === 'delete'
  const error = actionData?.intent === 'delete' ? actionData.message : null

  return (
    <Dialog.Root onOpenChange={(open) => !open && onClose()} open>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="m">
          <Form method="post">
            <input name="_intent" type="hidden" value="delete" />
            <input name="name" type="hidden" value={name} />
            <Dialog.Title>Delete {name}?</Dialog.Title>
            <Dialog.Description>
              This removes the function from this workspace. Queries that call it will stop working.
            </Dialog.Description>
            {error ? (
              <Typography.BodySmall as="p" role="alert" variant="error">
                {error}
              </Typography.BodySmall>
            ) : null}
            <Dialog.Actions>
              <Button.TextButton
                disabled={deleting}
                onClick={onClose}
                type="button"
                variant="secondary"
              >
                Cancel
              </Button.TextButton>
              <Button.TextButton disabled={deleting} type="submit" variant="destructive">
                {deleting ? 'Deleting…' : 'Delete function'}
              </Button.TextButton>
            </Dialog.Actions>
          </Form>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function FunctionField({ children, label }: { children: React.ReactNode; label: string }) {
  return (
    <label className={styles.field}>
      <Typography.BodySmallStrong>{label}</Typography.BodySmallStrong>
      {children}
    </label>
  )
}
