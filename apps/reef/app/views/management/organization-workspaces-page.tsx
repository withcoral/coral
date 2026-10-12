import { useRef, useState } from 'react'
import { Button, Menu, Table, Typography } from '@/wax/components'
import { StatePill } from './access'
import { ActionDialog, PersonField } from './forms'
import { ManagementLayout, PageIntro, trail } from './layout'
import { Person } from './person'
import type { FormContext, OrganizationPersonView, OrganizationWorkspaceView } from './types'
import * as styles from './management.css'

/**
 * Every workspace in the organization, for an organization owner. It shows
 * who owns each workspace, and nothing inside one.
 */
export function OrganizationWorkspacesPage({
  workspaces,
  candidates,
  currentUser,
  nextPage,
  form,
}: {
  workspaces: OrganizationWorkspaceView[]
  /** Every enabled organization member: the people who can own a workspace. */
  candidates: OrganizationPersonView[]
  currentUser: string
  nextPage: string
  form: FormContext
}) {
  return (
    <ManagementLayout breadcrumbs={trail([], 'Workspaces')}>
      <PageIntro>
        {workspaces.some((w) => w.state === 'archived') && (
          <Typography.BodySmall as="p">
            Restore an archived workspace before you assign an owner.
          </Typography.BodySmall>
        )}
      </PageIntro>
      <Table.Container
        ariaLabel="Workspaces"
        layout="fixed"
        columns={[
          { label: 'Name', width: { fill: 2 } },
          { label: 'State', width: 'fill' },
          { label: 'Owners', width: { fill: 2 } },
          { ariaLabel: 'Actions', width: 48 },
        ]}
      >
        <Table.Head />
        <Table.Body>
          {workspaces.length === 0 && <Table.Status>No workspaces on this page.</Table.Status>}
          {workspaces.map((w) => (
            <Table.Row key={w.name}>
              <Table.Cell wrap>{w.displayName}</Table.Cell>
              <Table.Cell wrap>
                <StatePill workspace={w} />
              </Table.Cell>
              <Table.Cell wrap>
                {w.owners.length === 0 ? (
                  'No owner'
                ) : (
                  <ul className={styles.owners} aria-label={`Owners of ${w.displayName}`}>
                    {w.owners.map((o) => (
                      <li key={o.user}>
                        <Person
                          person={o.profile}
                          currentUser={currentUser}
                          compact
                          disabled={!o.enabled}
                        />
                      </li>
                    ))}
                  </ul>
                )}
              </Table.Cell>
              <Table.Cell actions>
                <WorkspaceActions
                  workspace={w}
                  candidates={candidates}
                  currentUser={currentUser}
                  form={form}
                />
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Container>
      {nextPage && (
        <Button.InternalLink to={`?page=${encodeURIComponent(nextPage)}`}>
          Next workspaces
        </Button.InternalLink>
      )}
    </ManagementLayout>
  )
}

function WorkspaceActions({
  workspace: w,
  candidates,
  currentUser,
  form,
}: {
  workspace: OrganizationWorkspaceView
  candidates: OrganizationPersonView[]
  currentUser: string
  form: FormContext
}) {
  const [action, setAction] = useState<'assign' | 'archive' | 'restore' | null>(null)
  const [owner, setOwner] = useState('')
  const trigger = useRef<HTMLButtonElement>(null)
  const dialog = {
    assign: {
      title: 'Assign owner',
      intent: 'assignOwner',
      description: `Choose a person to own ${w.displayName}. Coral adds them to the workspace as an owner.`,
    },
    archive: {
      title: 'Archive workspace',
      intent: 'archiveWorkspace',
      description: `Archiving ${w.displayName} blocks new runs, unsent operation calls and settings changes. Coral keeps the history. You can restore the workspace.`,
    },
    restore: {
      title: 'Restore workspace',
      intent: 'restoreWorkspace',
      description: `Restore ${w.displayName} to allow new runs and settings changes. Other access restrictions still apply.`,
    },
  }[action ?? 'assign']
  return (
    <>
      <Menu.Container>
        <Menu.Trigger
          render={
            <Button.IconButton
              ref={trigger}
              name="Ellipsis"
              ariaLabel={`Actions for ${w.displayName}`}
              variant="bare"
            />
          }
        >
          {null}
        </Menu.Trigger>
        <Menu.Content>
          {w.state === 'active' ? (
            <>
              <Menu.Item
                onClick={() => {
                  setOwner('')
                  setAction('assign')
                }}
              >
                Assign owner
              </Menu.Item>
              <Menu.Item onClick={() => setAction('archive')}>Archive workspace</Menu.Item>
            </>
          ) : (
            <Menu.Item onClick={() => setAction('restore')}>Restore workspace</Menu.Item>
          )}
        </Menu.Content>
      </Menu.Container>
      <ActionDialog
        open={action !== null}
        onOpenChange={(open) => {
          if (!open) setAction(null)
        }}
        finalFocus={trigger}
        title={dialog.title}
        description={dialog.description}
        intent={dialog.intent}
        name={w.name}
        etag={w.etag}
        form={form}
        destructive={action === 'archive'}
        disabled={action === 'assign' && !owner}
      >
        {action === 'assign' && (
          <PersonField
            label="Owner"
            name="owner"
            people={candidates}
            currentUser={currentUser}
            onChange={setOwner}
          />
        )}
      </ActionDialog>
    </>
  )
}
