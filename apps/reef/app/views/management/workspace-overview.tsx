import { useRef, useState, type ReactNode } from 'react'
import { useNavigation } from 'react-router'
import { Button, Card, Typography } from '@/wax/components'
import { Logo } from '@/wax/components/logo'
import { StatusDot } from '@/wax/components/status'
import type { RecipeIcons, SourceChoice } from '@/lib/provider-recipes/choice'
import { AddProviderLauncher } from '@/views/providers/add-provider'
import { AgentKeyDialog } from './agent-key-dialog'
import { ActionDialog, NameInput } from './forms'
import { ManagementLayout, trail } from './layout'
import { NameHeading } from './name-heading'
import { OnboardingDialog } from './onboarding-dialog'
import { onboardingPrompt } from './onboarding-prompt'
import { Person } from './person'
import { WorkspaceQuickStart } from './quick-start'
import type {
  ActionResult,
  AgentView,
  FormContext,
  MembershipView,
  OwnerView,
  WorkspaceView,
} from './types'
import * as styles from './management.css'

export interface ProviderSummary {
  id: string
  displayName: string
  namespace: string
  path: string
  /** Why its tools cannot run now; empty when ready. */
  blockers: string[]
  icons?: RecipeIcons
}

export interface WorkspaceOverviewProps {
  workspace: WorkspaceView
  /** At most `GLIMPSE_FETCH` agents: the card needs no more to choose its rows. */
  agents: AgentView[]
  providers: ProviderSummary[]
  /** Null in a local deployment, which has no other people to show. */
  owners: OwnerView[] | null
  ownMembership: MembershipView | null
  form: FormContext
  /** What the Add provider dialog offers. */
  recipes: readonly SourceChoice[]
  /** Recent activity totals, supplied by the route. */
  summary?: ReactNode
  /** Whether this visit follows the workspace's creation, so onboarding opens. */
  onboarding?: boolean
  /** The MCP clients this deployment says may have a Coral plugin. The prompt asks them to look for it. */
  clientsWithAvailablePlugins: readonly string[]
}

/**
 * The Workspace home: a glimpse of its providers, agents and members, then its recent activity totals.
 * Each empty list offers the one action that fills it.
 */
export function WorkspaceOverview({
  workspace: w,
  agents,
  providers,
  owners,
  ownMembership,
  form,
  recipes,
  summary,
  onboarding = false,
  clientsWithAvailablePlugins,
}: WorkspaceOverviewProps) {
  const owner = w.effectiveRole === 'owner'
  const active = w.state === 'active'
  const canChange = owner && active
  const agentsPath = `${w.path}/agents`
  const pending = useNavigation().state !== 'idle'
  const [created, setCreated] = useState<ActionResult>()
  const [createOpen, setCreateOpen] = useState(false)
  const createTrigger = useRef<HTMLButtonElement>(null)
  // Revalidation reports no onboarding after the first load, so it must not close it.
  const [onboardingOpen, setOnboardingOpen] = useState(onboarding)
  // One prompt for the onboarding dialog and the quick start's last tab. Coral serves the
  // implicit local user only when it runs without authentication, and that same runtime
  // setting serves its MCP server with no sign-in.
  const prompt = onboardingPrompt({
    displayName: w.displayName,
    endpoint: w.mcpEndpoint,
    signIn: !w.profile.local,
    owner,
    clientsWithAvailablePlugins,
  })
  const members = owners?.map((o) => ({ profile: o.profile, user: o.user, enabled: o.enabled }))
  if (members && ownMembership && !members.some((m) => m.user === ownMembership.user))
    members.unshift({
      profile: ownMembership.profile,
      user: ownMembership.user,
      enabled: ownMembership.enabled,
    })
  return (
    <ManagementLayout breadcrumbs={trail([], 'Overview')} titled compact>
      <NameHeading
        name={w.displayName}
        noun="workspace"
        canRename={canChange}
        form={form}
        intent="rename"
        etag={w.etag}
        action={w.path}
        pageTitle
      />
      {/* Stays mounted whichever button opened it, so the one-time key survives revalidation. */}
      <ActionDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        finalFocus={createTrigger}
        title="Create agent"
        description="Use an agent for automations that run without you."
        form={form}
        intent="createAgent"
        action={agentsPath}
        disabled={!canChange}
        onSuccess={setCreated}
      >
        <NameInput label="Agent name" />
      </ActionDialog>
      <AgentKeyDialog
        credential={created?.credential ?? null}
        agentName={created?.agentDisplayName}
        endpoint={w.connectEndpoint}
        onClose={() => setCreated(undefined)}
      />
      {/* Without an MCP URL there is no prompt to give, so onboarding does not open. */}
      {w.mcpEndpoint && (
        <OnboardingDialog
          prompt={prompt}
          owner={owner}
          open={onboardingOpen}
          onClose={() => setOnboardingOpen(false)}
        />
      )}
      {active && w.connectEndpoint && (
        <WorkspaceQuickStart endpoint={w.connectEndpoint} prompt={prompt} owner={owner} />
      )}
      <div className={styles.cards}>
        <Card.Container className={styles.panel}>
          <div className={styles.sectionHeading}>
            <Typography.HeadingSmall as="h2">Providers</Typography.HeadingSmall>
            {providers.length > 0 && (
              <Button.IconLink
                name="ChevronRight"
                to={`${w.path}/providers`}
                tooltipText="View all providers"
              />
            )}
          </div>
          {providers.length === 0 ? (
            <Empty
              text={
                canChange
                  ? 'Add a provider so Coral can talk to its API.'
                  : 'Ask a workspace owner to add a provider so Coral can talk to its API.'
              }
              action={
                canChange && (
                  <AddProviderLauncher
                    form={form}
                    pending={pending}
                    newPath={`${w.path}/providers/new`}
                    recipes={recipes}
                  />
                )
              }
            />
          ) : (
            <Glimpse
              items={providers}
              itemKey={(provider) => provider.id}
              more={{ to: `${w.path}/providers`, label: 'View all providers' }}
              render={(provider) => (
                <>
                  <span className={styles.row}>
                    <Logo name={provider.displayName} icons={provider.icons} size="20" />
                    <Button.InternalLink to={provider.path} variant="link" size="small" wrap>
                      {provider.displayName}
                    </Button.InternalLink>
                  </span>
                  <StatusDot blockers={provider.blockers} />
                </>
              )}
            />
          )}
        </Card.Container>
        {owner && (
          <Card.Container className={styles.panel}>
            <div className={styles.sectionHeading}>
              <Typography.HeadingSmall as="h2">Agents</Typography.HeadingSmall>
              {agents.length > 0 && (
                <Button.IconLink
                  name="ChevronRight"
                  to={agentsPath}
                  tooltipText="View all agents"
                />
              )}
            </div>
            {agents.length === 0 ? (
              <Empty
                text="Create an agent for automations that run without you."
                action={
                  canChange && (
                    <Button.TextButton
                      ref={createTrigger}
                      variant="secondary"
                      size="32"
                      onClick={() => setCreateOpen(true)}
                    >
                      Create agent
                    </Button.TextButton>
                  )
                }
              />
            ) : (
              <Glimpse
                items={agents}
                itemKey={(a) => a.name}
                more={{ to: agentsPath, label: 'View all agents' }}
                render={(a) => (
                  <>
                    <Button.InternalLink to={a.path} variant="link" size="small" wrap>
                      {a.displayName}
                    </Button.InternalLink>
                    <StatusDot blockers={a.accessBlockers} />
                  </>
                )}
              />
            )}
          </Card.Container>
        )}
        {members && (
          <Card.Container className={styles.panel}>
            <div className={styles.sectionHeading}>
              <Typography.HeadingSmall as="h2">Members</Typography.HeadingSmall>
              <Button.IconLink
                name="ChevronRight"
                to={`${w.path}/members`}
                tooltipText={owner ? 'Manage members' : 'View members'}
              />
            </div>
            <Glimpse
              items={members}
              itemKey={(m) => m.user}
              more={{ to: `${w.path}/members`, label: 'View all members' }}
              render={(m) => (
                <Person
                  person={m.profile}
                  currentUser={w.currentUser}
                  compact
                  disabled={!m.enabled}
                />
              )}
            />
          </Card.Container>
        )}
      </div>
      {summary}
    </ManagementLayout>
  )
}

/** How many rows a card lists before it links to the full page. */
const GLIMPSE = 3
/** Enough rows to tell a list that shows whole from one that links to the rest. */
export const GLIMPSE_FETCH = GLIMPSE + 2

/** A card's first few rows, then a link to the page that lists them all. */
function Glimpse<T>({
  items,
  itemKey,
  render,
  more,
}: {
  items: readonly T[]
  itemKey: (item: T) => string
  render: (item: T) => ReactNode
  more: { to: string; label: string }
}) {
  // Hiding a single row saves no space, so a list one over the limit shows whole.
  const shown = items.length > GLIMPSE + 1 ? items.slice(0, GLIMPSE) : items
  return (
    <div className={styles.glimpse}>
      <ul className={styles.plainList}>
        {shown.map((item) => (
          <li key={itemKey(item)} className={styles.listRow}>
            {render(item)}
          </li>
        ))}
      </ul>
      {shown.length < items.length && (
        <Button.InternalLink
          to={more.to}
          variant="bare"
          size="x-small"
          className={styles.glimpseMore}
        >
          {more.label}
        </Button.InternalLink>
      )}
    </div>
  )
}

function Empty({ text, action }: { text: string; action?: ReactNode }) {
  return (
    <div className={styles.empty}>
      <Typography.BodySmall as="p">{text}</Typography.BodySmall>
      {action}
    </div>
  )
}
