/**
 * Throwaway prototype of the reworked onboarding flow from the Lagoon UI board:
 * https://www.figma.com/board/ucr862EnZFigUXN6FMjHj1/Lagoon-UI?node-id=326-594
 *
 * Written against the SQL-free Coral in `withcoral/lagoon`, so the copy speaks
 * about providers, operations, and TypeScript programs. There is no SQL, no
 * source spec, and no stdio MCP transport on this flow.
 *
 * Three steps: connect a provider, point an agent at the workspace MCP
 * endpoint, then run a prompt to prove it works. One story holds the whole flow
 * so it can be clicked through page by page. The floating pager at the bottom
 * right steps between pages in both directions.
 *
 * Nothing is wired to real data. The catalog and the add-provider state machine
 * live in local state, so it exists to review the copy and the shape of each
 * screen rather than to add a provider.
 *
 * Adding a provider assumes a catalog: an entry already carries its descriptor,
 * so the dialog opens on setup, then walks whose account it uses, what programs
 * call it, and the operation policy. Only a provider outside the catalog starts
 * on the URL-or-file step, which is the first step of the shipped add-source
 * dialog.
 *
 * Setup holds provider-level settings — a site, a region, a base URL — and
 * never a credential value. Credentials belong to an account, so they are
 * entered in the add-account screen.
 */
import { useRef, useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'

import type { CatalogEntry } from '@/lib/sources'
import { Button, Dialog, Inputs, Radio, ScrollArea, Tabs, Typography } from '@/wax/components'
import { CopyButton } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { theme } from '@/wax/theme/theme.css'

import { formatSourceName, ProviderLogo } from '@/components/sources'
import * as sourceAddStyles from '@/views/sources/source-add.css'
import { SourceField } from '@/views/sources/source-presentation'

import * as nextStepStyles from './onboarding-next-steps-page.css'
import { OnboardingLink, OnboardingPage } from './onboarding-page'
import { ONBOARDING_STEPS, type OnboardingStepState } from './onboarding-steps'

const FLOW_STEPS = ['providers', 'connect', 'try'] as const
type FlowStep = (typeof FLOW_STEPS)[number]

const FLOW_TITLE = 'Set up Coral'

/**
 * Stands in for the workspace MCP endpoint. Coral serves one route per
 * workspace, `/workspaces/{workspace_id}/mcp`, over Streamable HTTP.
 */
const WORKSPACE_MCP_URL =
  'https://coral.your-company.com/workspaces/3f9c1a7e-5d40-4c1b-9a2e-7c8f1b2d4e60/mcp'

const SKILL_INSTALL_COMMAND = 'npx skills add withcoral/skills'

const CLAUDE_CODE_COMMAND = `claude mcp add --transport http coral ${WORKSPACE_MCP_URL}`

function agentSetupPrompt(mcpUrl: string): string {
  return [
    'Help me connect you to Coral and make my first provider work.',
    '',
    'Coral is a data access layer for agents: one place to connect upstream services such as GitHub, Slack, Linear, and Datadog, then call their operations from small TypeScript programs that Coral runs for me.',
    '',
    'Set it up as follows:',
    `1. Add Coral as a remote MCP server over Streamable HTTP at ${mcpUrl}. Coral has no stdio transport, so do not look for a local command to run.`,
    '2. Sign in when Coral asks. The endpoint is protected by OAuth, and my browser has to complete it.',
    `3. Install the Coral agent skill with \`${SKILL_INSTALL_COMMAND}\`. It teaches you how to write Coral programs.`,
    '4. Most MCP clients only load servers at startup, so I may need to restart the client or open a new chat before the Coral tools appear. Tell me if that is needed and wait for me to do it.',
    `5. Once the tools are there, call \`search\` to find the operations I connected, \`describe\` one of them, then \`exec\` one small read-only program that uses it.`,
    '',
    'Make the changes yourself when you have terminal access, explain what you changed, and stop with a clear next step if anything needs my input.',
    '',
    'Docs: https://withcoral.com/docs',
    'Source (and the place to start if something breaks): https://github.com/withcoral/coral',
  ].join('\n')
}

// ---------------------------------------------------------------------------
// Fake catalog
//
// The catalog is the easy path: Coral already holds the descriptor and the
// credential contract for each entry, so adding one starts at its settings
// rather than at a URL. Everything here stands in for that catalog.
// ---------------------------------------------------------------------------

/** Where the descriptor came from, which decides the default operation policy. */
type ProviderSource = 'mcp' | 'openapi'

interface SpecField {
  hint?: string
  key: string
  label: string
  placeholder?: string
}

/**
 * One way to authenticate to the provider. The method is provider-level, since
 * the descriptor decides what the provider accepts. The values behind it are
 * not: an account holds those.
 */
interface AuthMethod {
  /** Collected per account, in the add-account dialog. */
  credentialFields: SpecField[]
  label: string
  value: string
}

interface ProviderSpec {
  authMethods: AuthMethod[]
  /**
   * Non-secret settings the provider needs before any account can use it: a
   * region, a site, a base URL. Collected once, in the setup dialog.
   */
  metadataFields: SpecField[]
  /** How many operations the descriptor holds. */
  operations: number
  source: ProviderSource
}

const OAUTH_METHOD: AuthMethod = {
  credentialFields: [
    {
      hint: 'From the OAuth app you registered with the provider.',
      key: 'client_id',
      label: 'Client ID',
    },
    { key: 'client_secret', label: 'Client secret' },
  ],
  label: 'OAuth',
  value: 'oauth',
}

const CUSTOM_AUTH_METHOD: AuthMethod = {
  credentialFields: [
    { key: 'header_name', label: 'Header name', placeholder: 'X-Api-Key' },
    { key: 'header_value', label: 'Header value' },
  ],
  label: 'Custom auth',
  value: 'custom',
}

const NO_AUTH_METHOD: AuthMethod = { credentialFields: [], label: 'None', value: 'none' }

const DEFAULT_SPEC: ProviderSpec = {
  authMethods: [
    OAUTH_METHOD,
    { credentialFields: [{ key: 'token', label: 'Token' }], label: 'Token', value: 'token' },
    CUSTOM_AUTH_METHOD,
    NO_AUTH_METHOD,
  ],
  metadataFields: [
    {
      hint: 'Where Coral sends every call for this provider.',
      key: 'base_url',
      label: 'API base URL',
      placeholder: 'https://api.example.com',
    },
  ],
  operations: 84,
  source: 'openapi',
}

const PROVIDER_SPECS: Record<string, ProviderSpec> = {
  claude: {
    authMethods: [
      {
        credentialFields: [{ key: 'api_key', label: 'API key', placeholder: 'sk-ant-…' }],
        label: 'Token',
        value: 'token',
      },
      CUSTOM_AUTH_METHOD,
    ],
    metadataFields: [
      { key: 'base_url', label: 'API base URL', placeholder: 'https://api.anthropic.com' },
      {
        hint: 'Scopes every call to one Claude workspace.',
        key: 'workspace_id',
        label: 'Workspace ID',
        placeholder: 'wrkspc_01…',
      },
    ],
    operations: 46,
    source: 'openapi',
  },
  datadog: {
    authMethods: [
      OAUTH_METHOD,
      {
        credentialFields: [
          { key: 'api_key', label: 'API key' },
          { key: 'app_key', label: 'Application key' },
        ],
        label: 'Token',
        value: 'token',
      },
      CUSTOM_AUTH_METHOD,
    ],
    metadataFields: [
      {
        hint: 'The regional site your organization lives on.',
        key: 'dd_site',
        label: 'Datadog site',
        placeholder: 'datadoghq.eu',
      },
      { key: 'default_env', label: 'Default environment tag', placeholder: 'prod' },
    ],
    operations: 612,
    source: 'openapi',
  },
  github: {
    authMethods: [
      OAUTH_METHOD,
      {
        credentialFields: [{ key: 'token', label: 'Personal access token', placeholder: 'ghp_…' }],
        label: 'Token',
        value: 'token',
      },
      CUSTOM_AUTH_METHOD,
    ],
    metadataFields: [
      {
        hint: 'Point this at your Enterprise Server to leave github.com alone.',
        key: 'base_url',
        label: 'API base URL',
        placeholder: 'https://api.github.com',
      },
      { key: 'default_org', label: 'Default organization', placeholder: 'withcoral' },
    ],
    operations: 1204,
    source: 'openapi',
  },
  linear: {
    authMethods: [
      OAUTH_METHOD,
      {
        credentialFields: [{ key: 'api_key', label: 'API key', placeholder: 'lin_api_…' }],
        label: 'Token',
        value: 'token',
      },
      CUSTOM_AUTH_METHOD,
    ],
    metadataFields: [
      { key: 'workspace_url', label: 'Workspace URL', placeholder: 'https://linear.app/acme' },
      {
        hint: 'Used when a program names no team.',
        key: 'default_team',
        label: 'Default team key',
        placeholder: 'ENG',
      },
    ],
    operations: 208,
    source: 'openapi',
  },
  linear_mcp: {
    authMethods: [OAUTH_METHOD, NO_AUTH_METHOD],
    metadataFields: [
      { key: 'server_url', label: 'MCP server URL', placeholder: 'https://mcp.linear.app/mcp' },
    ],
    operations: 24,
    source: 'mcp',
  },
  notion: {
    authMethods: [
      OAUTH_METHOD,
      {
        credentialFields: [
          { key: 'token', label: 'Internal integration token', placeholder: 'ntn_…' },
        ],
        label: 'Token',
        value: 'token',
      },
      CUSTOM_AUTH_METHOD,
    ],
    metadataFields: [
      { key: 'workspace_domain', label: 'Workspace domain', placeholder: 'acme.notion.site' },
      {
        hint: 'Leave empty to reach every page the account can see.',
        key: 'root_page_id',
        label: 'Root page ID',
      },
    ],
    operations: 97,
    source: 'openapi',
  },
  sentry_mcp: {
    authMethods: [OAUTH_METHOD, NO_AUTH_METHOD],
    metadataFields: [
      { key: 'server_url', label: 'MCP server URL', placeholder: 'https://mcp.sentry.dev/mcp' },
      { key: 'org_slug', label: 'Organization slug', placeholder: 'acme' },
    ],
    operations: 31,
    source: 'mcp',
  },
  slack: {
    authMethods: [
      OAUTH_METHOD,
      {
        credentialFields: [{ key: 'bot_token', label: 'Bot token', placeholder: 'xoxb-…' }],
        label: 'Token',
        value: 'token',
      },
      CUSTOM_AUTH_METHOD,
    ],
    metadataFields: [
      { key: 'workspace_domain', label: 'Workspace domain', placeholder: 'acme.slack.com' },
      {
        hint: 'Coral refuses to read channels outside this list.',
        key: 'allowed_channels',
        label: 'Allowed channels',
        placeholder: '#engineering, #incidents',
      },
    ],
    operations: 350,
    source: 'openapi',
  },
  the_times: {
    authMethods: [
      {
        credentialFields: [{ key: 'api_key', label: 'API key' }],
        label: 'Token',
        value: 'token',
      },
      CUSTOM_AUTH_METHOD,
    ],
    metadataFields: [
      { key: 'edition', label: 'Edition', placeholder: 'uk' },
      { key: 'base_url', label: 'API base URL', placeholder: 'https://api.nytimes.com' },
    ],
    operations: 18,
    source: 'openapi',
  },
}

function providerSpec(sourceName: string): ProviderSpec {
  return PROVIDER_SPECS[sourceName] ?? DEFAULT_SPEC
}

/** The mock draws a mixed catalog: OpenAPI documents and MCP servers. */
const CATALOG: CatalogEntry[] = [
  {
    description: 'Read issues, projects, and cycles over the Linear MCP server.',
    installed: false,
    name: 'linear_mcp',
    origin: 'imported',
    version: '0.4.0',
  },
  {
    description: 'Read issues, events, and releases over the Sentry MCP server.',
    installed: false,
    name: 'sentry_mcp',
    origin: 'imported',
    version: '0.2.0',
  },
  {
    description: 'Browse project issues, labels, milestones, and users.',
    installed: false,
    name: 'linear',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Read pages, databases, comments, and workspace content.',
    installed: false,
    name: 'notion',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Read channels, messages, and users from a Slack workspace.',
    installed: false,
    name: 'slack',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Read metrics, logs, monitors, and dashboards.',
    installed: false,
    name: 'datadog',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Read issues, pull requests, and code from your repositories.',
    installed: false,
    name: 'github',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Read conversations and usage from Claude.',
    installed: false,
    name: 'claude',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Search articles and archives from The Times.',
    installed: false,
    name: 'the_times',
    origin: 'imported',
    version: '0.1.0',
  },
]

/** One prompt per provider, so the last step can suggest what the user added. */
const PROVIDER_PROMPTS: Record<string, string> = {
  claude: 'Using Coral, show my Claude usage by day for the last two weeks.',
  datadog: 'Using Coral, list the Datadog monitors that alerted in the last 24 hours.',
  github: 'Using Coral, list the open pull requests in my repositories that wait on my review.',
  linear: 'Using Coral, list my in-progress Linear issues with their cycle and project.',
  linear_mcp: 'Using Coral, list my in-progress Linear issues with their cycle and project.',
  notion: 'Using Coral, find the Notion pages that changed in the last week.',
  sentry_mcp: 'Using Coral, list the Sentry issues that first appeared this week.',
  slack: 'Using Coral, summarise yesterday in my busiest Slack channel.',
  the_times: 'Using Coral, find the five most recent Times articles about my industry.',
}

const CATALOG_PROMPT =
  'Search Coral for the operations you can reach, describe one of them, then run a small program that returns a handful of rows from it.'

function tryPrompts(sourceNames: string[]): string[] {
  const fromProviders = sourceNames
    .map((sourceName) => PROVIDER_PROMPTS[sourceName])
    .filter((prompt): prompt is string => prompt !== undefined)
    .slice(0, 2)

  return [CATALOG_PROMPT, ...fromProviders]
}

// ---------------------------------------------------------------------------
// Flow state
// ---------------------------------------------------------------------------

/** The Credential Route, in the words Reef uses for it. */
type AccountChoice = 'caller' | 'fixed' | 'none'

const ACCOUNT_LABELS: Record<AccountChoice, string> = {
  caller: "Each person's own account",
  fixed: 'One shared account',
  none: 'No account',
}

const ACCOUNT_HINTS: Record<AccountChoice, string> = {
  caller: 'Each user signs in with their own account when they first use the provider.',
  fixed: 'All users in this workspace use one account.',
  none: 'No account is necessary.',
}

/**
 * A shared account Coral already holds for a provider. Coral keeps one per
 * upstream authorization, so a workspace can pick between several.
 */
interface Account {
  authMethod: string
  label: string
  /** How this account signs in, shown next to its name. */
  methodLabel: string
  sourceName: string
}

/** Accounts that pre-date this user, so the shared-account list has rows. */
const SEEDED_ACCOUNTS: Account[] = [
  { authMethod: 'oauth', label: 'slack_bot', methodLabel: 'OAuth', sourceName: 'slack' },
  { authMethod: 'token', label: 'slack_readonly', methodLabel: 'Token', sourceName: 'slack' },
  { authMethod: 'token', label: 'linear_bot', methodLabel: 'Token', sourceName: 'linear' },
]

/** The Workspace's Operation Access State, which Reef sets per operation. */
type PolicyValue = 'approval' | 'disabled' | 'enabled'

/**
 * What Coral derives from the descriptor when no rule says otherwise: a
 * read-only operation is enabled, anything else waits for approval.
 */
const DEFAULT_POLICY: PolicyValue = 'approval'

const POLICY_LABELS: Record<PolicyValue, string> = {
  approval: 'Approval required',
  disabled: 'Disabled',
  enabled: 'Enabled',
}

interface Provider {
  account: AccountChoice
  accountLabel: string | null
  operations: number
  policy: PolicyValue
  /** The immutable namespace programs call it by. */
  programName: string
  source: ProviderSource
  sourceName: string
}

/**
 * Coral takes each operation's effect classification from its descriptor. An
 * MCP tool can declare itself read-only, so it starts enabled; every
 * OpenAPI-sourced operation is unknown in v1, so it waits for approval.
 */
function approvalNote(source: ProviderSource): string {
  return source === 'mcp'
    ? 'read-only operations enabled, the rest need approval'
    : 'every operation needs approval until you enable it'
}

const panelBodyStyle: React.CSSProperties = {
  boxSizing: 'border-box',
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  padding: 16,
}

interface FlowArgs {
  /** Which page the story opens on. Handy for jumping straight to a later step. */
  startStep: FlowStep
}

const meta = {
  args: {
    startStep: 'providers',
  },
  argTypes: {
    startStep: {
      control: 'radio',
      options: FLOW_STEPS,
    },
  },
  parameters: {
    layout: 'fullscreen',
  },
  render: (args) => <OnboardingFlow {...args} />,
  title: 'Components/Onboarding/NewOnboarding',
} satisfies Meta<FlowArgs>

export default meta
type Story = StoryObj<typeof meta>

export const NewFlow: Story = {}

function OnboardingFlow({ startStep }: FlowArgs) {
  const [stepIndex, setStepIndex] = useState(FLOW_STEPS.indexOf(startStep))
  const [search, setSearch] = useState('')

  const [providers, setProviders] = useState<Provider[]>([])
  const [accounts, setAccounts] = useState<Account[]>(SEEDED_ACCOUNTS)
  /** The catalog entry being added, or `''` for the blank "Add provider" path. */
  const [adding, setAdding] = useState<string | null>(null)

  const step = FLOW_STEPS[stepIndex]!
  const stepState = flowStepState(step)
  const goNext = () => setStepIndex((index) => Math.min(index + 1, FLOW_STEPS.length - 1))
  const goBack = () => setStepIndex((index) => Math.max(index - 1, 0))

  return (
    <>
      {step === 'providers' ? (
        <ProvidersStep
          onAdd={(sourceName) => setAdding(sourceName)}
          onNext={goNext}
          onSearchChange={setSearch}
          providers={providers}
          search={search}
          step={stepState}
        />
      ) : null}

      {step === 'connect' ? <ConnectStep onNext={goNext} step={stepState} /> : null}

      {step === 'try' ? (
        <TryStep
          prompts={tryPrompts(providers.map((provider) => provider.sourceName))}
          step={stepState}
        />
      ) : null}

      <AddProviderDialog
        accounts={accounts}
        onAddAccount={(account) => setAccounts((current) => [...current, account])}
        onAdded={(provider) =>
          setProviders((current) => [
            ...current.filter((existing) => existing.programName !== provider.programName),
            provider,
          ])
        }
        onClose={() => setAdding(null)}
        sourceName={adding}
      />

      <FlowPager
        canGoBack={stepIndex > 0}
        canGoNext={stepIndex < FLOW_STEPS.length - 1}
        label={`${stepIndex + 1} / ${FLOW_STEPS.length}`}
        onBack={goBack}
        onNext={goNext}
      />
    </>
  )
}

// ---------------------------------------------------------------------------
// Pages
// ---------------------------------------------------------------------------

function ProvidersStep({
  onAdd,
  onNext,
  onSearchChange,
  providers,
  search,
  step,
}: {
  onAdd: (sourceName: string) => void
  onNext: () => void
  onSearchChange: (search: string) => void
  providers: Provider[]
  search: string
  step: OnboardingStepState
}) {
  const matches = searchMatcher(search)
  const added = providers.map((provider) => provider.sourceName)
  const catalogEntries = CATALOG.filter((entry) => !added.includes(entry.name)).filter(matches)

  return (
    <OnboardingPage
      action={{
        disabled: providers.length === 0,
        label: 'I have connected a provider',
        onClick: onNext,
      }}
      ariaLabel="Connect your providers"
      step={step}
      sideContent={
        <>
          <Typography.BodyLarge>
            A provider is a service Coral calls for you: GitHub, Slack, Datadog, your own API. Coral
            turns each one into a catalog of operations.
          </Typography.BodyLarge>
          <Typography.BodyLarge>
            Your agents call those operations from small programs they write for you, as{' '}
            <Typography.CodeInline as="code">
              coral.providers.github.issues.list
            </Typography.CodeInline>
            . Coral runs the program and keeps the credentials hidden from your agent.{' '}
            <OnboardingLink href="https://withcoral.com/docs">Learn more</OnboardingLink>
          </Typography.BodyLarge>
        </>
      }
      sideTitle="Connect your providers"
      title={FLOW_TITLE}
    >
      <PanelFrame
        actions={
          <>
            <PanelSearch onChange={onSearchChange} value={search} />
            <Button.TextButton onClick={() => onAdd('')} size="22" variant="secondary">
              Add provider
            </Button.TextButton>
          </>
        }
        title="Providers"
      >
        <ScrollArea.Container constrainWidth fillContent style={{ flex: 1, minHeight: 0 }}>
          <div style={{ ...panelBodyStyle, gap: 22 }}>
            {providers.length > 0 ? (
              <PanelSection count={providers.length} title="Added">
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {providers.map((provider) => (
                    <ProviderRow key={provider.programName} provider={provider} />
                  ))}
                </div>
              </PanelSection>
            ) : null}

            <PanelSection count={catalogEntries.length} title="Catalog">
              <ProviderCardList
                actionLabel="Add"
                entries={catalogEntries}
                onAction={(entry) => onAdd(entry.name)}
              />
            </PanelSection>
          </div>
        </ScrollArea.Container>
      </PanelFrame>
    </OnboardingPage>
  )
}

function ConnectStep({ onNext, step }: { onNext: () => void; step: OnboardingStepState }) {
  const prompt = agentSetupPrompt(WORKSPACE_MCP_URL)

  return (
    <OnboardingPage
      action={{ label: 'I have connected to Coral', onClick: onNext }}
      ariaLabel="Connect your agent to Coral"
      step={step}
      sideContent={
        <>
          <Typography.BodyLarge>
            Coral serves this workspace as one MCP server over HTTP. Point your agent at the
            workspace URL and sign in.
          </Typography.BodyLarge>
          <Typography.BodyLarge>
            Let an agent do the setup, or add the URL to your client by hand. We recommend
            installing the{' '}
            <OnboardingLink href="https://withcoral.com/docs/getting-started/installation#skills">
              Coral skill
            </OnboardingLink>{' '}
            for the best results.
          </Typography.BodyLarge>
        </>
      }
      sideTitle="Connect your agent to Coral"
      title={FLOW_TITLE}
    >
      <div className={nextStepStyles.panel}>
        <Tabs.Root className={nextStepStyles.tabs} defaultValue="ai-assisted">
          <Tabs.List aria-label="Coral setup method" className={nextStepStyles.tabList}>
            <Tabs.Tab value="ai-assisted">AI assisted</Tabs.Tab>
            <Tabs.Tab value="manual">Manual</Tabs.Tab>
            <Tabs.Indicator />
          </Tabs.List>

          <Tabs.Panel className={nextStepStyles.tabPanel} value="ai-assisted">
            <header className={nextStepStyles.panelHeader}>
              <Typography.HeadingXSmall as="h2">Set up with your agent</Typography.HeadingXSmall>
              <Typography.Body variant="tertiary">
                Copy this prompt into a coding agent that can access your terminal.
              </Typography.Body>
            </header>

            <PromptField ariaLabel="Coral agent setup prompt" prompt={prompt} />
          </Tabs.Panel>

          <Tabs.Panel className={nextStepStyles.tabPanel} value="manual">
            <section className={nextStepStyles.manualSection}>
              <header className={nextStepStyles.panelHeader}>
                <Typography.HeadingXSmall as="h2">
                  1. Add this workspace to your client
                </Typography.HeadingXSmall>
                <Typography.Body variant="tertiary">
                  Add it as a remote MCP server. Your client opens a browser to sign in the first
                  time it connects.
                </Typography.Body>
              </header>

              <CommandField ariaLabel="Workspace MCP URL" command={WORKSPACE_MCP_URL} />

              <Typography.Body variant="tertiary">
                In Claude Code, run{' '}
                <Typography.CodeInline as="code">{CLAUDE_CODE_COMMAND}</Typography.CodeInline>. For
                every other client, see the{' '}
                <OnboardingLink href="https://withcoral.com/docs/guides/use-coral-over-mcp">
                  MCP setup guide
                </OnboardingLink>
                .
              </Typography.Body>
            </section>

            <section className={nextStepStyles.manualSection}>
              <header className={nextStepStyles.panelHeader}>
                <Typography.HeadingXSmall as="h2">
                  2. Install the Coral skill
                </Typography.HeadingXSmall>
                <Typography.Body variant="tertiary">
                  This teaches your agent how to write Coral programs.
                </Typography.Body>
              </header>

              <CommandField
                ariaLabel="Coral skill install command"
                command={SKILL_INSTALL_COMMAND}
              />
            </section>
          </Tabs.Panel>
        </Tabs.Root>
      </div>
    </OnboardingPage>
  )
}

function TryStep({ prompts, step }: { prompts: string[]; step: OnboardingStepState }) {
  return (
    <OnboardingPage
      action={{ label: "Take me to Coral's dashboard", onClick: () => undefined }}
      ariaLabel="Try Coral from your agent"
      step={step}
      sideContent={
        <>
          <Typography.BodyLarge>
            Try asking your agent a question about the providers you connected, or copy one of these
            example prompts!
          </Typography.BodyLarge>
          <Typography.BodyLarge>
            Read the <OnboardingLink href="https://withcoral.com/docs">docs</OnboardingLink> for
            more information, or join us on{' '}
            <OnboardingLink href="https://withcoral.com/discord">Discord</OnboardingLink>!
          </Typography.BodyLarge>
        </>
      }
      sideTitle="Try your first prompt"
      title={FLOW_TITLE}
    >
      <div className={nextStepStyles.panel}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
          <header className={nextStepStyles.panelHeader}>
            <Typography.HeadingXSmall as="h2">Example prompts</Typography.HeadingXSmall>
            <Typography.Body variant="tertiary">
              Copy one into your agent and watch it call Coral.
            </Typography.Body>
          </header>

          {prompts.map((prompt, index) => (
            <PromptField ariaLabel={`Example prompt ${index + 1}`} key={prompt} prompt={prompt} />
          ))}
        </div>
      </div>
    </OnboardingPage>
  )
}

// ---------------------------------------------------------------------------
// Add provider
//
// A catalog entry already carries its descriptor, so it opens straight on
// credentials. Anything outside the catalog starts on the URL-or-file step that
// `views/sources/source-add.tsx` already ships, then joins the same chain:
// credentials, whose account does it use, and what programs call it.
// ---------------------------------------------------------------------------

type DialogScreen = 'account' | 'name' | 'new-account' | 'policy' | 'setup' | 'source'

function AddProviderDialog({
  accounts,
  onAddAccount,
  onAdded,
  onClose,
  sourceName,
}: {
  accounts: Account[]
  onAddAccount: (account: Account) => void
  onAdded: (provider: Provider) => void
  onClose: () => void
  sourceName: string | null
}) {
  return (
    <Dialog.Root onOpenChange={(open) => !open && onClose()} open={sourceName !== null}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="m">
          {sourceName !== null ? (
            <AddProviderFlow
              accounts={accounts}
              key={sourceName}
              onAddAccount={onAddAccount}
              onAdded={onAdded}
              onClose={onClose}
              sourceName={sourceName}
            />
          ) : null}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function AddProviderFlow({
  accounts,
  onAddAccount,
  onAdded,
  onClose,
  sourceName,
}: {
  accounts: Account[]
  onAddAccount: (account: Account) => void
  onAdded: (provider: Provider) => void
  onClose: () => void
  /** A catalog entry, or `''` when the user started from "Add provider". */
  sourceName: string
}) {
  const fromCatalog = sourceName !== ''
  const spec = fromCatalog ? providerSpec(sourceName) : DEFAULT_SPEC
  const providerName = fromCatalog ? formatSourceName(sourceName) : 'this provider'

  const [screen, setScreen] = useState<DialogScreen>(fromCatalog ? 'setup' : 'source')
  const [url, setUrl] = useState('')
  const [authMethod, setAuthMethod] = useState(spec.authMethods[0]?.value ?? 'oauth')
  const [metadata, setMetadata] = useState<Record<string, string>>({})
  const [credentials, setCredentials] = useState<Record<string, string>>({})
  const [account, setAccount] = useState<AccountChoice>('fixed')
  const [accountLabel, setAccountLabel] = useState<string | null>(null)
  const [newAccountName, setNewAccountName] = useState('')
  const [programName, setProgramName] = useState(sourceName)
  const [policy, setPolicy] = useState<PolicyValue>(DEFAULT_POLICY)

  const method = spec.authMethods.find((candidate) => candidate.value === authMethod)
  const resolvedProgramName = programName.trim() || 'provider'
  const validProgramName = /^[A-Za-z][A-Za-z0-9_]*$/.test(resolvedProgramName)

  // Every account Coral already holds for this provider. Each one carries the
  // method it was authorized with, so the list does not filter by method.
  const matchingAccounts = accounts.filter((candidate) => candidate.sourceName === sourceName)

  const complete = (nextPolicy: PolicyValue) => {
    onAdded({
      account,
      accountLabel: account === 'fixed' ? accountLabel : null,
      operations: spec.operations,
      policy: nextPolicy,
      programName: resolvedProgramName,
      source: spec.source,
      sourceName: sourceName || resolvedProgramName,
    })
    onClose()
  }

  if (screen === 'source') {
    return (
      <SourceStep
        onCancel={onClose}
        onNext={() => setScreen('setup')}
        onUrlChange={setUrl}
        url={url}
      />
    )
  }

  // Settings are the non-secret answers Coral needs before it can call the
  // provider at all: which site, which region, which base URL. Credentials are
  // not settings, so they live on an account instead.
  if (screen === 'setup') {
    return (
      <>
        <Dialog.Title>{`${providerName} settings`}</Dialog.Title>
        <Dialog.Description>
          {`These tell Coral which ${providerName} to call. Nothing here is secret — you add credentials with an account in the next step.`}
        </Dialog.Description>
        <Dialog.Close />

        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          {spec.metadataFields.map((field) => (
            <Field hint={field.hint} key={field.key} label={field.label}>
              <Inputs.TextInput
                onChange={(value) => setMetadata({ ...metadata, [field.key]: value })}
                placeholder={field.placeholder}
                value={metadata[field.key] ?? ''}
              />
            </Field>
          ))}

          {spec.metadataFields.length === 0 ? (
            <Typography.BodySmall variant="tertiary">
              {`${providerName} needs no settings.`}
            </Typography.BodySmall>
          ) : null}
        </div>

        <Dialog.Actions>
          <Button.TextButton
            onClick={() => (fromCatalog ? onClose() : setScreen('source'))}
            variant="secondary"
          >
            {fromCatalog ? 'cancel' : 'back'}
          </Button.TextButton>
          <Button.TextButton onClick={() => setScreen('account')}>next</Button.TextButton>
        </Dialog.Actions>
      </>
    )
  }

  if (screen === 'new-account') {
    const fields = method?.credentialFields ?? []

    return (
      <>
        <Dialog.Title>{`Add an account for ${providerName}`}</Dialog.Title>
        <Dialog.Description>
          Name the account, choose how it signs in, and enter its credentials. Coral keeps them
          encrypted and never hands them to a program.
        </Dialog.Description>
        <Dialog.Close />

        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <Field label="Account name">
            <Inputs.TextInput
              autoFocus
              onChange={setNewAccountName}
              placeholder={`${sourceName || 'provider'}_bot`}
              value={newAccountName}
            />
          </Field>

          {/* Which methods exist comes from the descriptor; which one this
              account uses is chosen here, next to its credentials. */}
          <Field label="How it signs in">
            <Tabs.Root onValueChange={(value) => setAuthMethod(String(value))} value={authMethod}>
              <Tabs.List aria-label="How it signs in" className={nextStepStyles.tabList}>
                {spec.authMethods.map((candidate) => (
                  <Tabs.Tab key={candidate.value} value={candidate.value}>
                    {candidate.label}
                  </Tabs.Tab>
                ))}
                <Tabs.Indicator />
              </Tabs.List>
            </Tabs.Root>
          </Field>

          {fields.map((field) => (
            <Field hint={field.hint} key={field.key} label={field.label}>
              <Inputs.TextInput
                onChange={(value) => setCredentials({ ...credentials, [field.key]: value })}
                placeholder={field.placeholder}
                value={credentials[field.key] ?? ''}
              />
            </Field>
          ))}

          {fields.length === 0 ? (
            <Typography.BodySmall variant="tertiary">
              This method needs no credentials.
            </Typography.BodySmall>
          ) : null}
        </div>

        <Dialog.Actions>
          <Button.TextButton onClick={() => setScreen('account')} variant="secondary">
            back
          </Button.TextButton>
          <Button.TextButton
            onClick={() => {
              const label = newAccountName.trim() || `${sourceName || 'provider'}_bot`
              onAddAccount({
                authMethod,
                label,
                methodLabel: method?.label ?? authMethod,
                sourceName,
              })
              setAccountLabel(label)
              setScreen('account')
            }}
          >
            save
          </Button.TextButton>
        </Dialog.Actions>
      </>
    )
  }

  if (screen === 'account') {
    return (
      <>
        <Dialog.Title>Whose account does it use?</Dialog.Title>
        <Dialog.Description>
          Coral holds the credentials. A program never sees them and cannot pick another account.
        </Dialog.Description>
        <Dialog.Close />

        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <Radio.Group
            aria-label="Whose account does it use?"
            onValueChange={(value) => setAccount(value as AccountChoice)}
            style={{ alignItems: 'flex-start', flexDirection: 'column', gap: 12 }}
            value={account}
          >
            <Radio.Item value="fixed">{ACCOUNT_LABELS.fixed}</Radio.Item>
            <Radio.Item value="caller">{ACCOUNT_LABELS.caller}</Radio.Item>
            <Radio.Item value="none">{ACCOUNT_LABELS.none}</Radio.Item>
          </Radio.Group>

          <Typography.BodySmall variant="tertiary">{ACCOUNT_HINTS[account]}</Typography.BodySmall>

          {account === 'fixed' ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <div
                style={{
                  border: `1px solid ${theme.stroke.secondary}`,
                  borderRadius: 8,
                  display: 'flex',
                  flexDirection: 'column',
                  overflow: 'hidden',
                }}
              >
                {matchingAccounts.map((candidate) => (
                  <AccountRow
                    account={candidate}
                    key={candidate.label}
                    onSelect={() => setAccountLabel(candidate.label)}
                    selected={candidate.label === accountLabel}
                  />
                ))}
                <button
                  onClick={() => {
                    setNewAccountName('')
                    setScreen('new-account')
                  }}
                  style={{
                    background: 'none',
                    border: 'none',
                    borderBlockStart:
                      matchingAccounts.length > 0
                        ? `1px solid ${theme.stroke.secondary}`
                        : undefined,
                    color: theme.content.link,
                    cursor: 'pointer',
                    font: 'inherit',
                    paddingBlock: 10,
                    paddingInline: 12,
                    textAlign: 'center',
                  }}
                  type="button"
                >
                  <Typography.Body>+ add new account</Typography.Body>
                </button>
              </div>

              <Typography.BodySmall variant="tertiary">
                {matchingAccounts.length > 0
                  ? `Coral already holds these accounts for ${providerName}.`
                  : `Coral holds no account for ${providerName} yet.`}
              </Typography.BodySmall>
            </div>
          ) : null}
        </div>

        <Dialog.Actions>
          <Button.TextButton onClick={() => setScreen('setup')} variant="secondary">
            back
          </Button.TextButton>
          <Button.TextButton
            disabled={account === 'fixed' && accountLabel === null}
            onClick={() => setScreen('name')}
          >
            next
          </Button.TextButton>
        </Dialog.Actions>
      </>
    )
  }

  if (screen === 'name') {
    return (
      <>
        <Dialog.Title>Name it</Dialog.Title>
        <Dialog.Description>
          Programs in this workspace call the provider by the name you give it.
        </Dialog.Description>
        <Dialog.Close />

        <Field
          hint={
            validProgramName
              ? `Programs use this name: coral.providers.${resolvedProgramName}. Use letters, digits and underscores. Start with a letter. You cannot change it later.`
              : 'Use letters, digits and underscores. Start with a letter.'
          }
          label="Program name"
        >
          <Inputs.TextInput
            autoFocus
            onChange={setProgramName}
            placeholder="github"
            value={programName}
          />
        </Field>

        <Dialog.Actions>
          <Button.TextButton onClick={() => setScreen('account')} variant="secondary">
            back
          </Button.TextButton>
          <Button.TextButton disabled={!validProgramName} onClick={() => setScreen('policy')}>
            next
          </Button.TextButton>
        </Dialog.Actions>
      </>
    )
  }

  return (
    <>
      <Dialog.Title>{`Add ${providerName}`}</Dialog.Title>
      <Dialog.Description>
        {`Set the policy for all ${spec.operations} operations of this provider, or keep the default. ${
          spec.source === 'mcp'
            ? 'Coral enables the operations this server declares read-only and asks you to approve the rest.'
            : 'An OpenAPI document does not say which operations are read-only, so Coral asks you to approve every one of them.'
        }`}
      </Dialog.Description>
      <Dialog.Close />

      <Radio.Group
        aria-label="Operation policy"
        onValueChange={(value) => setPolicy(value as PolicyValue)}
        style={{ alignItems: 'flex-start', flexDirection: 'column', gap: 12 }}
        value={policy}
      >
        <Radio.Item value="enabled">{POLICY_LABELS.enabled}</Radio.Item>
        <Radio.Item value="approval">{POLICY_LABELS.approval}</Radio.Item>
        <Radio.Item value="disabled">{POLICY_LABELS.disabled}</Radio.Item>
      </Radio.Group>

      <Dialog.Actions>
        <Button.TextButton onClick={() => setScreen('name')} variant="secondary">
          back
        </Button.TextButton>
        {/* Leaving the radios alone keeps the policy Coral derived from the
            descriptor. Touching them rewrites it for every operation, so the
            button says which of the two the click does. */}
        <Button.TextButton onClick={() => complete(policy)}>
          {policy === DEFAULT_POLICY ? 'keep defaults' : 'apply changes'}
        </Button.TextButton>
      </Dialog.Actions>
    </>
  )
}

function AccountRow({
  account,
  onSelect,
  selected,
}: {
  account: Account
  onSelect: () => void
  selected: boolean
}) {
  return (
    <button
      onClick={onSelect}
      style={{
        alignItems: 'center',
        background: selected ? theme.surface.onMainContent : 'none',
        border: 'none',
        cursor: 'pointer',
        display: 'flex',
        font: 'inherit',
        gap: 8,
        justifyContent: 'space-between',
        paddingBlock: 10,
        paddingInline: 12,
        textAlign: 'start',
      }}
      type="button"
    >
      <Typography.Body>
        {account.label}{' '}
        <Typography.Body as="span" variant="tertiary">{`(${account.methodLabel})`}</Typography.Body>
      </Typography.Body>
      {selected ? <Icon color="primary" name="Check" size="16" /> : null}
    </button>
  )
}

/**
 * The first step of the shipped add-source dialog, borrowed whole: a URL, or a
 * file dropped or picked. Only a provider outside the catalog needs it.
 *
 * @see app/views/sources/source-add.tsx
 */
function SourceStep({
  onCancel,
  onNext,
  onUrlChange,
  url,
}: {
  onCancel: () => void
  onNext: () => void
  onUrlChange: (url: string) => void
  url: string
}) {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [dropping, setDropping] = useState(false)
  const [fileName, setFileName] = useState('')
  const ready = url.trim().startsWith('https://') || fileName !== ''

  return (
    <>
      <Dialog.Title>Add provider</Dialog.Title>
      <Dialog.Description>
        Coral reads the document one time and turns each endpoint into an operation.
      </Dialog.Description>
      <Dialog.Close />

      <div className={sourceAddStyles.fieldGroup}>
        <SourceField
          hint={
            <Typography.BodySmall variant="tertiary">
              Enter an OpenAPI document or streamable HTTP MCP endpoint.
            </Typography.BodySmall>
          }
          htmlFor="onboarding-provider-url"
          label="Provider URL"
        >
          <Inputs.TextInput
            id="onboarding-provider-url"
            onChange={(value) => {
              onUrlChange(value)
              setFileName('')
            }}
            placeholder="https://example.com/openapi.yaml"
            value={url}
          />
        </SourceField>
      </div>

      <div className={sourceAddStyles.orDivider}>
        <Typography.BodySmall variant="tertiary">or</Typography.BodySmall>
      </div>

      <input
        accept=".json,.yaml,.yml"
        className={sourceAddStyles.hiddenFileInput}
        onChange={(event) => {
          setFileName(event.currentTarget.files?.[0]?.name ?? '')
          onUrlChange('')
        }}
        ref={fileInputRef}
        tabIndex={-1}
        type="file"
      />
      <div
        className={sourceAddStyles.manifestDropZone}
        data-dropping={dropping || undefined}
        onDragEnter={() => setDropping(true)}
        onDragLeave={() => setDropping(false)}
        onDragOver={(event) => event.preventDefault()}
        onDrop={(event) => {
          event.preventDefault()
          setDropping(false)
          setFileName(event.dataTransfer.files[0]?.name ?? '')
          onUrlChange('')
        }}
      >
        <Icon color="secondary" name="FileCode" size="30" />
        <Typography.Body>{fileName || 'Drop a document here'}</Typography.Body>
        <Button.Container
          onClick={() => fileInputRef.current?.click()}
          size="32"
          variant="secondary"
        >
          <Button.Icon name="Upload" />
          <Button.Text>Choose a file</Button.Text>
        </Button.Container>
      </div>

      <Dialog.Actions>
        <Button.TextButton onClick={onCancel} variant="secondary">
          cancel
        </Button.TextButton>
        <Button.TextButton disabled={!ready} onClick={onNext}>
          next
        </Button.TextButton>
      </Dialog.Actions>
    </>
  )
}

// ---------------------------------------------------------------------------
// Panel pieces
// ---------------------------------------------------------------------------

/** Forked from SourceCardList so each card can carry its own Add button. */
function ProviderCardList({
  actionLabel,
  entries,
  onAction,
}: {
  actionLabel: string
  entries: CatalogEntry[]
  onAction: (entry: CatalogEntry) => void
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {entries.map((entry) => {
        return (
          <div
            key={entry.name}
            style={{
              alignItems: 'center',
              background: theme.surface.onMainContent,
              border: `1px solid ${theme.stroke.secondary}`,
              borderRadius: 8,
              display: 'flex',
              gap: 12,
              paddingBlock: 10,
              paddingInline: 12,
            }}
          >
            <ProviderLogo name={entry.name} size="small" />
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
              <Typography.BodyStrong truncate>{formatSourceName(entry.name)}</Typography.BodyStrong>
              <Typography.BodySmall truncate variant="tertiary">
                {entry.description}
              </Typography.BodySmall>
            </div>
            <Button.TextButton onClick={() => onAction(entry)} size="22" variant="secondary">
              {actionLabel}
            </Button.TextButton>
          </div>
        )
      })}
    </div>
  )
}

function ProviderRow({ provider }: { provider: Provider }) {
  const detail = [
    `${provider.operations} operations`,
    provider.account === 'fixed' && provider.accountLabel
      ? `shared account: ${provider.accountLabel}`
      : ACCOUNT_LABELS[provider.account].toLowerCase(),
    provider.policy === 'approval'
      ? approvalNote(provider.source)
      : POLICY_LABELS[provider.policy].toLowerCase(),
  ].join(' · ')

  return (
    <div
      style={{
        alignItems: 'center',
        background: theme.surface.onMainContent,
        border: `1px solid ${theme.stroke.secondary}`,
        borderRadius: 8,
        display: 'flex',
        gap: 12,
        paddingBlock: 10,
        paddingInline: 12,
      }}
    >
      <ProviderLogo name={provider.sourceName} size="small" />
      <div style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
        <Typography.BodyStrong truncate>
          {`coral.providers.${provider.programName}`}
        </Typography.BodyStrong>
        <Typography.BodySmall truncate variant="tertiary">
          {detail}
        </Typography.BodySmall>
      </div>
      <Icon color="primary" name="Check" size="16" />
    </div>
  )
}

/** The framed panel on the right of every step, with its own title row. */
function PanelFrame({
  actions,
  children,
  title,
}: {
  actions?: React.ReactNode
  children: React.ReactNode
  title: string
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      <div
        style={{
          alignItems: 'center',
          borderBlockEnd: `1px solid ${theme.stroke.secondary}`,
          display: 'flex',
          flexShrink: 0,
          gap: 16,
          justifyContent: 'space-between',
          paddingBlock: 16,
          paddingInline: 16,
        }}
      >
        <Typography.HeadingXSmall as="h2">{title}</Typography.HeadingXSmall>
        <div style={{ alignItems: 'center', display: 'flex', gap: 8, minWidth: 0 }}>{actions}</div>
      </div>
      <div style={{ display: 'flex', flex: 1, flexDirection: 'column', minHeight: 0 }}>
        {children}
      </div>
    </div>
  )
}

function PanelSearch({ onChange, value }: { onChange: (value: string) => void; value: string }) {
  return (
    <div style={{ maxWidth: 280, width: '100%' }}>
      <Inputs.TextInput
        icon="Search"
        onChange={onChange}
        placeholder="Search providers…"
        value={value}
      />
    </div>
  )
}

function PanelSection({
  children,
  count,
  title,
}: {
  children: React.ReactNode
  count: number
  title: string
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div style={{ alignItems: 'baseline', display: 'flex', gap: 8 }}>
        <Typography.HeadingXSmall as="h3">{title}</Typography.HeadingXSmall>
        <Typography.BodySmall variant="tertiary">{count}</Typography.BodySmall>
      </div>
      {children}
    </div>
  )
}

function Field({
  children,
  hint,
  label,
}: {
  children: React.ReactNode
  hint?: string
  label: string
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      <Typography.BodySmallStrong>{label}</Typography.BodySmallStrong>
      {children}
      {hint ? <Typography.BodySmall variant="tertiary">{hint}</Typography.BodySmall> : null}
    </div>
  )
}

function CommandField({ ariaLabel, command }: { ariaLabel: string; command: string }) {
  return (
    <div className={nextStepStyles.commandField}>
      <Inputs.TextInput
        ariaLabel={ariaLabel}
        className={nextStepStyles.commandInput}
        readOnly
        value={command}
      />
      <CopyButton
        ariaLabel={`Copy ${ariaLabel}`}
        className={nextStepStyles.copyButton}
        textToCopy={command}
        variant="bare"
      />
    </div>
  )
}

function PromptField({ ariaLabel, prompt }: { ariaLabel: string; prompt: string }) {
  return (
    <div className={nextStepStyles.promptField}>
      <ScrollArea.Container
        className={nextStepStyles.promptScrollArea}
        constrainWidth
        fade="none"
        fillContent
      >
        <pre className={nextStepStyles.promptText}>{prompt}</pre>
      </ScrollArea.Container>
      <CopyButton
        ariaLabel={`Copy ${ariaLabel}`}
        className={nextStepStyles.copyButton}
        textToCopy={prompt}
        variant="bare"
      />
    </div>
  )
}

/** Story-only affordance: the mock has no back button, but reviewing needs one. */
function FlowPager({
  canGoBack,
  canGoNext,
  label,
  onBack,
  onNext,
}: {
  canGoBack: boolean
  canGoNext: boolean
  label: string
  onBack: () => void
  onNext: () => void
}) {
  return (
    <div
      style={{
        alignItems: 'center',
        background: theme.surface.onMainContent,
        border: `1px solid ${theme.stroke.secondary}`,
        borderRadius: 999,
        display: 'flex',
        gap: 8,
        insetBlockEnd: 16,
        insetInlineEnd: 16,
        paddingBlock: 6,
        paddingInline: 10,
        position: 'fixed',
        zIndex: 10,
      }}
    >
      <Button.Container disabled={!canGoBack} onClick={onBack} size="22" variant="bare">
        <Button.Icon name="ChevronLeft" />
      </Button.Container>
      <Typography.BodySmall variant="tertiary">{label}</Typography.BodySmall>
      <Button.Container disabled={!canGoNext} onClick={onNext} size="22" variant="bare">
        <Button.Icon name="ChevronRight" />
      </Button.Container>
    </div>
  )
}

function searchMatcher(search: string) {
  const query = search.trim().toLowerCase()

  return (entry: CatalogEntry) =>
    !query ||
    entry.name.toLowerCase().includes(query) ||
    entry.description.toLowerCase().includes(query)
}

function flowStepState(step: FlowStep): OnboardingStepState {
  const index = FLOW_STEPS.indexOf(step)

  return {
    current: index + 1,
    nextHref: null,
    nextStep: null,
    step: ONBOARDING_STEPS[index] ?? ONBOARDING_STEPS[0],
    total: FLOW_STEPS.length,
  }
}
