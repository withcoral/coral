import { useId } from 'react'
import { Button, Card, Tabs, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { PromptPreview } from './prompt-preview'
import * as styles from './management.css'

export type Client = 'claude' | 'codex' | 'other'

export const CLIENTS = [
  { value: 'claude', label: 'Claude Code' },
  { value: 'codex', label: 'Codex' },
  { value: 'other', label: 'Other' },
] as const

export const KEY = '<AGENT_API_KEY>'

export interface Step {
  /** What to do with the value, above it. */
  label: string
  /** The copy button's accessible name. */
  copy: string
  /** What the copy button copies. */
  snippet: string
  /** What the page shows: the snippet with a real key masked. */
  shown: string
  /** What else to do, under the value. */
  hint?: string
  /** Whether the value is a prompt, shown as its first lines; the copy holds all of it. */
  prose?: boolean
}

/**
 * Each client's setup for an agent, as values to copy. With the real key, they
 * carry it and nothing needs replacing.
 */
export function agentSteps(client: Client, endpoint: string, key?: string): Step[] {
  const k = key ?? KEY
  const list: Omit<Step, 'shown'>[] = (() => {
    switch (client) {
      case 'claude':
        return [
          {
            label: 'Run in a terminal',
            copy: 'Copy Claude Code command',
            snippet: `claude mcp add --transport http coral ${endpoint} --header "Authorization: Bearer ${k}"`,
          },
        ]
      case 'codex':
        // Codex reads an HTTP server's bearer token from an environment variable
        // its config names, so the key stays out of the file.
        return [
          {
            label: 'Add to ~/.codex/config.toml',
            copy: 'Copy Codex configuration',
            snippet: `[mcp_servers.coral]\nurl = "${endpoint}"\nbearer_token_env_var = "CORAL_API_KEY"`,
            hint: 'Set CORAL_API_KEY to the API key where Codex runs.',
          },
        ]
      case 'other':
        return [
          {
            label: 'MCP URL',
            copy: 'Copy MCP URL',
            snippet: endpoint,
            hint: "Add this URL to the agent's MCP client.",
          },
          {
            label: 'Authorization header',
            copy: 'Copy Authorization header',
            snippet: `Bearer ${k}`,
            hint: 'Send this header with every request.',
          },
        ]
    }
  })()
  const mask = (text: string) => (key ? text.replaceAll(key, '•'.repeat(12)) : text)
  return list.map((step) => ({ ...step, shown: mask(step.snippet) }))
}

/** Each client's setup for a person, who signs in with Coral instead of sending a key. */
export function personSteps(client: Client, endpoint: string): Step[] {
  const list: Omit<Step, 'shown'>[] = (() => {
    switch (client) {
      case 'claude':
        return [
          {
            label: 'Run in a terminal',
            copy: 'Copy Claude Code command',
            snippet: `claude mcp add --transport http coral ${endpoint}`,
          },
          {
            label: 'Then sign in',
            copy: 'Copy Claude Code sign-in command',
            snippet: 'claude mcp login coral',
            hint: 'Sign in with Coral when your browser opens.',
          },
        ]
      case 'codex':
        return [
          {
            label: 'Run in a terminal',
            copy: 'Copy Codex command',
            snippet: `codex mcp add coral --url ${endpoint}`,
          },
          {
            label: 'Then sign in',
            copy: 'Copy Codex sign-in command',
            snippet: 'codex mcp login coral',
            hint: 'Sign in with Coral when your browser opens.',
          },
        ]
      case 'other':
        return [
          {
            label: 'MCP URL',
            copy: 'Copy MCP URL',
            snippet: endpoint,
            hint: 'Add this URL to your MCP client as a Streamable HTTP server.',
          },
        ]
    }
  })()
  return list.map((step) => ({ ...step, shown: step.snippet }))
}

function StepValue({ step }: { step: Step }) {
  const label = useId()
  return (
    <li className={styles.intro}>
      <Typography.BodySmallStrong as="p" id={label}>
        {step.label}
      </Typography.BodySmallStrong>
      <div role="group" aria-labelledby={label}>
        {step.prose ? (
          <PromptPreview prompt={step.snippet} copyLabel={step.copy} />
        ) : (
          <CodeBlock value={step.shown} copy={{ text: step.snippet, label: step.copy }} />
        )}
      </div>
      {step.hint && (
        <Typography.BodySmall as="p" variant="tertiary">
          {step.hint}
        </Typography.BodySmall>
      )}
    </li>
  )
}

/** The tab after the clients' with a prompt that has your own MCP client do the setup. */
const ASK = 'ask'

/** Tabs per MCP client, each with the values to copy, then the prompt when there is one. */
export function ClientSetup({
  steps,
  prompt,
}: {
  steps: (client: Client) => Step[]
  prompt?: string
}) {
  return (
    <Tabs.Root className={styles.quickStartTabs} defaultValue="claude">
      <Tabs.List aria-label="MCP client" inset="none">
        {CLIENTS.map(({ value, label }) => (
          <Tabs.Tab key={value} value={value}>
            {label}
          </Tabs.Tab>
        ))}
        {prompt && <Tabs.Tab value={ASK}>Ask your agent</Tabs.Tab>}
        <Tabs.Indicator />
      </Tabs.List>
      <div className={styles.quickStartPanels}>
        {CLIENTS.map(({ value }) => (
          <Tabs.Panel key={value} value={value} keepMounted className={styles.quickStartPanel}>
            <ol className={styles.quickStartSteps}>
              {steps(value).map((step) => (
                <StepValue key={step.label} step={step} />
              ))}
            </ol>
          </Tabs.Panel>
        ))}
        {prompt && (
          <Tabs.Panel value={ASK} keepMounted className={styles.quickStartPanel}>
            <ol className={styles.quickStartSteps}>
              <StepValue
                step={{
                  label: 'Paste this prompt into your MCP client',
                  copy: 'Copy prompt',
                  snippet: prompt,
                  shown: prompt,
                  hint: 'Get your agent to connect to Coral and add a provider.',
                  prose: true,
                }}
              />
            </ol>
          </Tabs.Panel>
        )}
      </div>
    </Tabs.Root>
  )
}

/** How an agent's MCP client connects with its API key, until the agent has activity. */
export function AgentQuickStart({
  endpoint,
  settingsPath,
}: {
  endpoint: string
  settingsPath: string
}) {
  const heading = useId()
  return (
    <Card.Container as="section" className={styles.quickStart} aria-labelledby={heading}>
      <div className={styles.quickStartHeader}>
        <Typography.HeadingSmall as="h2" id={heading}>
          Connect this agent
        </Typography.HeadingSmall>
        <Typography.BodySmall as="p" variant="tertiary" className={styles.quickStartLost}>
          Lost the key?{' '}
          <Button.InternalLink to={settingsPath} variant="link" size="x-small">
            Replace it in settings
          </Button.InternalLink>
        </Typography.BodySmall>
      </div>
      <Typography.BodySmall as="p">
        Add this workspace to the agent's MCP client. Replace {KEY} with the key you saved. The
        agent's calls then show below.
      </Typography.BodySmall>
      <ClientSetup steps={(client) => agentSteps(client, endpoint)} />
    </Card.Container>
  )
}

/**
 * How a person connects their MCP client to the Workspace, until they have activity.
 * Its last tab holds the onboarding prompt, so it outlasts the dialog that first shows it.
 */
export function WorkspaceQuickStart({
  endpoint,
  prompt,
}: {
  endpoint: string
  /** The workspace's `onboardingPrompt`. */
  prompt: string
}) {
  const heading = useId()
  return (
    <Card.Container as="section" className={styles.overviewQuickStart} aria-labelledby={heading}>
      <Typography.HeadingSmall as="h2" id={heading}>
        Connect your MCP client
      </Typography.HeadingSmall>
      <Typography.BodySmall as="p">Add this workspace to your MCP client.</Typography.BodySmall>
      <ClientSetup steps={(client) => personSteps(client, endpoint)} prompt={prompt} />
    </Card.Container>
  )
}
