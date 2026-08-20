import { McpClientsList, type McpClientsConnectionState } from '@/components/mcp-clients-list'
import { Inputs, ScrollArea, Tabs, Typography } from '@/wax/components'
import { CopyButton } from '@/wax/components/button'

import { OnboardingLink, OnboardingPage } from './onboarding-page'
import * as styles from './onboarding-next-steps-page.css'
import type { OnboardingStepState } from './onboarding-steps'

export const CORAL_SKILL_INSTALL_COMMAND = 'npx skills add withcoral/skills --skill coral --global'

export function coralAgentSetupPrompt(runtime: 'desktop' | 'web'): string {
  const runtimeGuidance =
    runtime === 'desktop'
      ? [
          'I have completed Coral onboarding and connected at least one source. I am running Coral Desktop with its bundled Coral runtime.',
          '',
          "Use Coral Desktop's bundled Coral executable for the local stdio MCP server. Resolve its actual path rather than assuming `coral` is on my agent's PATH.",
          '',
          'Set it up as follows:',
        ]
      : [
          'I have completed Coral onboarding and connected at least one source. Start by identifying how I am running Coral. Talk it through with me, inspect my environment when possible, and recommend a setup path that uses my existing installation:',
          '',
          'Coral entrypoints:',
          '- Desktop app: a native app with a bundled Coral runtime for a regular desktop environment.',
          '- CLI (`coral`): for a terminal, headless machine, or server environment.',
          'Both use local Coral state and can expose the same stdio MCP server to a coding agent.',
          '',
          'How to think about it:',
          '- If I am using Coral Desktop, start with its bundled runtime. Do not require a separate CLI installation without explaining why it is needed.',
          '- If the `coral` CLI is installed, resolve its actual executable path rather than assuming my agent inherits the terminal PATH.',
          '',
          'Once you know which form I am using:',
        ]

  return [
    'Help me set up Coral and get my first source working with you.',
    '',
    'Coral is an open-source data access layer for AI agents: one place to connect sources such as GitHub, Slack, Linear, Datadog, and local files, then query them with SQL.',
    '',
    ...runtimeGuidance,
    '1. Connect to Coral over local stdio MCP. Find the actual Coral executable first, configure it as the command, and pass `mcp-stdio` as a separate argument. Do not point to a missing command or treat `mcp-stdio` as an npm package.',
    `2. Install only the \`coral\` agent skill globally from https://github.com/withcoral/skills. Use \`${CORAL_SKILL_INSTALL_COMMAND}\` when the official skills installer is available.`,
    '3. Most MCP clients only load servers at startup, so I may need to restart the client or open a new chat before the Coral tools appear. Tell me if that is needed and wait for me to do it before continuing.',
    '4. Once the tools are available, list the Coral catalog, find the source I connected during onboarding, and run one small read-only query to verify the setup end to end.',
    '',
    'Make the changes yourself when you have terminal access, explain what you changed, and stop with a clear next step if anything requires my input.',
    '',
    'Installation and skills: https://withcoral.com/docs/getting-started/installation',
    'MCP setup: https://withcoral.com/docs/guides/use-coral-over-mcp',
    'Source (and the place to start if something breaks): https://github.com/withcoral/coral',
  ].join('\n')
}

interface ConnectClientsProps {
  mcpClients: McpClientsConnectionState
  workspaces: ReadonlyArray<{ name: string }>
}

/** Only Desktop can write a client's config, so only Desktop carries the clients. */
export type OnboardingNextStepsPageProps = {
  completing?: boolean
  onContinue?: () => void
  step: OnboardingStepState
} & (
  | ({ runtime: 'desktop' } & ConnectClientsProps)
  | ({ runtime: 'web' } & Partial<Record<keyof ConnectClientsProps, never>>)
)

export function OnboardingNextStepsPage({
  completing = false,
  mcpClients,
  onContinue,
  runtime,
  step,
  workspaces,
}: OnboardingNextStepsPageProps) {
  const agentSetupPrompt = coralAgentSetupPrompt(runtime)

  return (
    <OnboardingPage
      action={{
        disabled: completing,
        label: completing ? 'Finishing setup…' : "Take me to Coral's dashboard",
        onClick: onContinue,
      }}
      ariaLabel="Set up Coral with an agent"
      step={step}
      sideContent={
        <>
          <Typography.BodyLarge>
            Coral is built for agents and works over MCP or the CLI, regardless of which agent you
            use. We recommend installing the{' '}
            <OnboardingLink href="https://withcoral.com/docs/getting-started/installation#skills">
              Coral skill
            </OnboardingLink>{' '}
            for the best results.
          </Typography.BodyLarge>
          <Typography.BodyLarge>
            Read the <OnboardingLink href="https://withcoral.com/docs">docs</OnboardingLink> for
            more information, or join us on{' '}
            <OnboardingLink href="https://withcoral.com/discord">Discord</OnboardingLink>!
          </Typography.BodyLarge>
        </>
      }
      sideTitle="Teach your agents how to use Coral"
    >
      <div className={styles.panel}>
        <Tabs.Root className={styles.tabs} defaultValue="ai-assisted">
          <Tabs.List aria-label="Coral setup method" className={styles.tabList}>
            <Tabs.Tab value="ai-assisted">AI-assisted</Tabs.Tab>
            <Tabs.Tab value="manual">Manual</Tabs.Tab>
            <Tabs.Indicator />
          </Tabs.List>

          <Tabs.Panel className={styles.tabPanel} value="manual">
            <section className={styles.manualSection}>
              <header className={styles.panelHeader}>
                <Typography.HeadingXSmall as="h2">
                  1. Connect your agent over MCP
                </Typography.HeadingXSmall>
              </header>

              {runtime === 'desktop' ? (
                <ConnectClients mcpClients={mcpClients} workspaces={workspaces} />
              ) : (
                <ManualConnectInstructions />
              )}
            </section>

            <section className={styles.manualSection}>
              <header className={styles.panelHeader}>
                <Typography.HeadingXSmall as="h2">
                  2. Install the Coral skill
                </Typography.HeadingXSmall>
                <Typography.Body variant="tertiary">
                  Run this command in your terminal to teach your agent how to use Coral.
                </Typography.Body>
              </header>

              <div className={styles.commandField}>
                <Inputs.TextInput
                  ariaLabel="Coral skill install command"
                  className={styles.commandInput}
                  readOnly
                  value={CORAL_SKILL_INSTALL_COMMAND}
                />
                <CopyButton
                  ariaLabel="Copy Coral skill install command"
                  className={styles.copyButton}
                  textToCopy={CORAL_SKILL_INSTALL_COMMAND}
                  variant="bare"
                />
              </div>
            </section>
          </Tabs.Panel>

          <Tabs.Panel className={styles.tabPanel} value="ai-assisted">
            <header className={styles.panelHeader}>
              <Typography.HeadingXSmall as="h2">Set up with your agent</Typography.HeadingXSmall>
              <Typography.Body variant="tertiary">
                Copy this prompt into a coding agent that can access your terminal.
              </Typography.Body>
            </header>

            <div className={styles.promptField}>
              <ScrollArea.Container
                className={styles.promptScrollArea}
                constrainWidth
                data-testid="coral-agent-setup-prompt"
                fade="none"
                fillContent
              >
                <pre className={styles.promptText}>{agentSetupPrompt}</pre>
              </ScrollArea.Container>
              <CopyButton
                ariaLabel="Copy Coral agent setup prompt"
                className={styles.copyButton}
                textToCopy={agentSetupPrompt}
                variant="bare"
              />
            </div>
          </Tabs.Panel>
        </Tabs.Root>
      </div>
    </OnboardingPage>
  )
}

function ConnectClients({ mcpClients, workspaces }: ConnectClientsProps) {
  return (
    <>
      <Typography.Body variant="tertiary">
        Connecting a client lets it query your sources over MCP.
      </Typography.Body>

      {/* Bounded so a long client list scrolls inside the card instead of pushing step 2
          out of the panel. */}
      <McpClientsList {...mcpClients} maxHeight={260} workspaces={workspaces} />

      <Typography.Body variant="tertiary">
        Only global MCP configurations appear here. See the{' '}
        <OnboardingLink href="https://withcoral.com/docs/guides/use-coral-over-mcp">
          MCP setup guide
        </OnboardingLink>{' '}
        for supported clients and troubleshooting.
      </Typography.Body>
    </>
  )
}

function ManualConnectInstructions() {
  return (
    <>
      <Typography.Body variant="tertiary">
        In your agent&apos;s MCP settings, add a local stdio server named{' '}
        <Typography.CodeInline as="code">coral</Typography.CodeInline>.
      </Typography.Body>

      <Typography.BodySmall variant="tertiary">
        Open this step in Coral Desktop to connect a client for you. This browser cannot configure a
        local MCP server, and web agents cannot launch a local stdio one.
      </Typography.BodySmall>

      <Typography.Body variant="tertiary">
        See the{' '}
        <OnboardingLink href="https://withcoral.com/docs/guides/use-coral-over-mcp">
          MCP setup guide
        </OnboardingLink>{' '}
        for supported clients and troubleshooting.
      </Typography.Body>
    </>
  )
}
