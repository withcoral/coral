import { describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-react'

import type { SourceCatalogEntry } from '@/components/sources'

import { OnboardingSampleQueryPage } from './onboarding-sample-query-page'
import { getOnboardingStepState } from './onboarding-steps'

const queryStep = getOnboardingStepState('query')

const github: SourceCatalogEntry = {
  description: 'Sync issues, pull requests, and code from your repositories.',
  installed: true,
  name: 'github',
  origin: 'bundled',
  version: '1.0.0',
}

const slack: SourceCatalogEntry = {
  description: 'Query messages and metadata from Slack.',
  installed: true,
  name: 'slack',
  origin: 'bundled',
  version: '1.0.0',
}

describe('OnboardingSampleQueryPage', () => {
  it('describes one connected source in the singular', async () => {
    const screen = await render(
      <OnboardingSampleQueryPage
        connectedSources={[github]}
        loadState="success"
        rows={[{ source: 'github', tables: 12 }]}
        step={queryStep}
      />,
    )

    await expect.element(screen.getByText('Your source is ready')).toBeVisible()
    await expect.element(screen.getByRole('cell', { name: 'github' })).toBeVisible()
    await expect.element(screen.getByRole('cell', { name: '12' })).toBeVisible()
  })

  it('describes multiple connected sources in the plural', async () => {
    const screen = await render(
      <OnboardingSampleQueryPage
        connectedSources={[github, slack]}
        loadState="success"
        rows={[
          { source: 'github', tables: 12 },
          { source: 'slack', tables: 2 },
        ]}
        step={queryStep}
      />,
    )

    await expect.element(screen.getByText('Your sources are ready')).toBeVisible()
    await expect.element(screen.getByRole('cell', { name: 'slack' })).toBeVisible()
  })

  it('retries a failed catalog query', async () => {
    const onRetry = vi.fn()
    const screen = await render(
      <OnboardingSampleQueryPage
        connectedSources={[github]}
        errorMessage="Catalog unavailable."
        loadState="error"
        onRetry={onRetry}
        step={queryStep}
      />,
    )

    await screen.getByRole('button', { name: 'Retry' }).click()

    expect(onRetry).toHaveBeenCalledOnce()
  })

  it('allows setup to continue when the catalog query succeeds without result metadata', async () => {
    const screen = await render(
      <OnboardingSampleQueryPage
        connectedSources={[github]}
        loadState="success"
        step={queryStep}
      />,
    )

    await expect.element(screen.getByRole('button', { name: 'Continue' })).toBeEnabled()
    await expect.element(screen.getByText('Results')).toBeVisible()
    await expect.element(screen.getByText('No rows returned.')).toBeVisible()
  })

  it('keeps the query visible when the results overflow the panel', async () => {
    const screen = await render(
      <OnboardingSampleQueryPage
        connectedSources={[github]}
        loadState="success"
        rows={Array.from({ length: 24 }, (_, index) => ({
          source: `source-${index}`,
          tables: index,
        }))}
        step={queryStep}
      />,
    )
    const queryPre = screen.getByText('SELECT', { exact: true }).element().closest('pre')
    const queryBlock = queryPre?.parentElement

    if (!queryPre || !queryBlock) throw new Error('Expected the rendered SQL block')

    expect(queryBlock.getBoundingClientRect().height).toBeGreaterThanOrEqual(
      queryPre.getBoundingClientRect().height,
    )
    await expect.element(screen.getByRole('cell', { name: 'source-23' })).toBeInTheDocument()
  })

  it('keeps setup blocked when no sources are connected', async () => {
    const screen = await render(
      <OnboardingSampleQueryPage connectedSources={[]} loadState="success" step={queryStep} />,
    )

    await expect.element(screen.getByRole('button', { name: 'Continue' })).toBeDisabled()
    await expect.element(screen.getByText('No connected sources')).toBeVisible()
  })
})
