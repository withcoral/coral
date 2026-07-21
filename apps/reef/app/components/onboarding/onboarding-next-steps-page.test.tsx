import { createRoutesStub } from 'react-router'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import {
  CORAL_SKILL_INSTALL_COMMAND,
  OnboardingNextStepsPage,
  coralAgentSetupPrompt,
} from './onboarding-next-steps-page'
import { getOnboardingStepState } from './onboarding-steps'

const nextStepsStep = getOnboardingStepState('next-steps')

describe('OnboardingNextStepsPage', () => {
  it('defaults to AI-assisted setup and offers manual setup', async () => {
    const desktopPrompt = coralAgentSetupPrompt('desktop')
    expect(desktopPrompt).toContain('--skill coral --global')
    expect(desktopPrompt).toContain('`mcp-stdio` as a separate argument')
    expect(desktopPrompt).toContain('I am running Coral Desktop')
    expect(desktopPrompt).not.toContain('Start by identifying how I am running Coral')
    expect(desktopPrompt.indexOf('1. Connect to Coral')).toBeLessThan(
      desktopPrompt.indexOf('2. Install only the `coral` agent skill'),
    )
    expect(desktopPrompt).toContain('https://withcoral.com/docs/guides/use-coral-over-mcp')

    const Stub = createRoutesStub([
      {
        Component: () => (
          <OnboardingNextStepsPage
            mcpLaunchConfig={{
              config: {
                args: ['mcp-stdio'],
                command: '/Applications/Coral.app/Contents/Resources/coral/coral',
              },
              status: 'success',
            }}
            onContinue={() => undefined}
            runtime="desktop"
            step={nextStepsStep}
          />
        ),
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect
      .element(screen.getByRole('link', { exact: true, name: 'Coral skill' }))
      .toHaveAttribute('href', 'https://withcoral.com/docs/getting-started/installation#skills')
    await expect
      .element(screen.getByRole('link', { exact: true, name: 'docs' }))
      .toHaveAttribute('href', 'https://withcoral.com/docs')
    await expect
      .element(screen.getByRole('link', { exact: true, name: 'Discord' }))
      .toHaveAttribute('href', 'https://withcoral.com/discord')
    await expect
      .element(screen.getByRole('button', { name: "Take me to Coral's dashboard" }))
      .toBeVisible()

    const aiAssistedTab = screen.getByRole('tab', { name: 'AI-assisted' })
    const manualTab = screen.getByRole('tab', { name: 'Manual' })

    await expect.element(aiAssistedTab).toHaveAttribute('aria-selected', 'true')
    const agentSetupPrompt = screen.getByRole('textbox', {
      exact: true,
      name: 'Coral agent setup prompt',
    })
    await expect.element(agentSetupPrompt).toHaveAttribute('aria-readonly', 'true')
    expect(agentSetupPrompt.element().textContent).toBe(desktopPrompt)
    await expect
      .poll(
        () =>
          agentSetupPrompt.element().querySelector<HTMLElement>('[data-has-overflow-y]') !== null,
      )
      .toBe(true)
    await expect
      .element(screen.getByRole('button', { name: 'Copy Coral agent setup prompt' }))
      .toBeVisible()

    await manualTab.click()
    await expect.element(manualTab).toHaveAttribute('aria-selected', 'true')
    const skillCommand = screen.getByRole('textbox', {
      exact: true,
      name: 'Coral skill install command',
    })
    await expect.element(skillCommand).toHaveValue(CORAL_SKILL_INSTALL_COMMAND)
    await expect.element(skillCommand).toHaveAttribute('readonly')
    await expect
      .element(screen.getByRole('button', { name: 'Copy Coral skill install command' }))
      .toBeVisible()
    await expect
      .element(screen.getByRole('heading', { name: '1. Connect your agent over MCP' }))
      .toBeVisible()
    await expect
      .element(screen.getByRole('heading', { name: '2. Install the Coral skill' }))
      .toBeVisible()
    expect(
      screen.getByLabelText('Coral MCP server configuration', { exact: true }).element()
        .textContent,
    ).toBe('command: "/Applications/Coral.app/Contents/Resources/coral/coral"\nargs: ["mcp-stdio"]')
    await expect
      .element(screen.getByRole('button', { name: 'Copy Coral MCP server configuration' }))
      .toBeVisible()
    await expect
      .element(screen.getByRole('link', { name: 'MCP setup guide' }))
      .toHaveAttribute('href', 'https://withcoral.com/docs/guides/use-coral-over-mcp')
    await expect.element(screen.getByText(/Restart your agent or open a new chat/)).toBeVisible()
  })

  it('does not invent a local executable path on the web', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => (
          <OnboardingNextStepsPage
            mcpLaunchConfig={{ status: 'unavailable' }}
            runtime="web"
            step={nextStepsStep}
          />
        ),
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    expect(
      screen.getByRole('textbox', { name: 'Coral agent setup prompt' }).element().textContent,
    ).toContain('Start by identifying how I am running Coral')

    await screen.getByRole('tab', { name: 'Manual' }).click()

    await expect
      .element(screen.getByText(/web agents cannot launch a local stdio MCP server/))
      .toBeVisible()
    await expect
      .element(screen.getByLabelText('Coral MCP server configuration', { exact: true }))
      .not.toBeInTheDocument()
  })
})
