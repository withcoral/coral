import { StrictMode } from 'react'
import { describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-react'

import type { SourceCatalogEntry } from '@/components/sources'

import { ONBOARDING_SAMPLE_QUERY, OnboardingSampleQueryPage } from './onboarding-sample-query-page'

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
      />,
    )

    await expect.element(screen.getByText('Your sources are ready')).toBeVisible()
    await expect.element(screen.getByRole('cell', { name: 'slack' })).toBeVisible()
  })

  it('runs the fixed catalog query once when StrictMode replays effects', async () => {
    const onRunSampleQuery = vi.fn()

    await render(
      <StrictMode>
        <OnboardingSampleQueryPage
          connectedSources={[github]}
          loadState="idle"
          onRunSampleQuery={onRunSampleQuery}
        />
      </StrictMode>,
    )

    await expect.poll(() => onRunSampleQuery.mock.calls.length).toBe(1)
    expect(onRunSampleQuery).toHaveBeenCalledWith(ONBOARDING_SAMPLE_QUERY)
  })

  it('retries a failed catalog query', async () => {
    const onRunSampleQuery = vi.fn()
    const screen = await render(
      <OnboardingSampleQueryPage
        connectedSources={[github]}
        errorMessage="Catalog unavailable."
        loadState="error"
        onRunSampleQuery={onRunSampleQuery}
      />,
    )

    await screen.getByRole('button', { name: 'Retry' }).click()

    expect(onRunSampleQuery).toHaveBeenCalledOnce()
    expect(onRunSampleQuery).toHaveBeenCalledWith(ONBOARDING_SAMPLE_QUERY)
  })

  it('allows setup to finish when the catalog query succeeds without result metadata', async () => {
    const screen = await render(
      <OnboardingSampleQueryPage connectedSources={[github]} loadState="success" />,
    )

    await expect.element(screen.getByRole('button', { name: 'Finish setup' })).toBeEnabled()
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
      <OnboardingSampleQueryPage connectedSources={[]} loadState="success" />,
    )

    await expect.element(screen.getByRole('button', { name: 'Finish setup' })).toBeDisabled()
    await expect.element(screen.getByText('No connected sources')).toBeVisible()
  })
})
