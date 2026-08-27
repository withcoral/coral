import { useEffect, useRef } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'

import type { SearchResultsView } from './search-response'
import { SearchResponseResults } from './search-response-results'

const groupedView: SearchResultsView = {
  results: [
    {
      description: 'Workflow jobs and their current conclusions.',
      fields: [
        { dataType: 'Utf8', name: 'owner', required: true },
        { dataType: 'Utf8', name: 'conclusion', required: false },
        { dataType: 'Utf8', name: 'repository', required: false },
      ],
      guide: 'Filter by owner before querying recent jobs.',
      kind: 'table',
      matchingValues: [
        { field: 'owner', values: ['withcoral'] },
        { field: 'conclusion', values: ['success', 'failure'] },
      ],
      omittedMatchingFieldCount: 2,
      providers: [
        { label: 'Catalog', tone: 'catalog' },
        { label: 'Observed values', tone: 'observed' },
      ],
      sqlReference: '"github"."repo_action_jobs"',
    },
    {
      arguments: [
        { dataType: 'Utf8', name: 'query', required: true },
        {
          dataType: 'Struct<repository_owner_namespace_with_a_very_long_nested_type_name>',
          name: 'required_argument_with_a_very_long_unbroken_name_that_wraps',
          required: true,
        },
        { dataType: 'Int64', name: 'z_limit', required: false },
      ],
      description: 'Searches issues using a provider-native function.',
      guide: '',
      kind: 'function',
      matchingValues: [],
      omittedMatchingFieldCount: 0,
      providers: [{ label: 'Native fanout', tone: 'neutral' }],
      returns: [
        { dataType: 'Int64', name: 'issue_id', required: false },
        { dataType: 'Utf8', name: 'title', required: false },
      ],
      sqlReference:
        '"Warehouse-Prod"."analytics"."Search Issues"(query, required_argument_with_a_very_long_unbroken_name_that_wraps, ...)',
    },
    { kind: 'unknown' },
  ],
  state: 'available',
  truncation: {
    maxResults: 3,
    note: 'More candidates were available.',
    returnedCount: 3,
    truncated: true,
  },
}

const withinLimitView = {
  ...groupedView,
  truncation: {
    maxResults: 10,
    note: '',
    returnedCount: 3,
    truncated: false,
  },
} satisfies SearchResultsView

function ExpandedResults({ view }: { view: SearchResultsView }) {
  const rootRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    rootRef.current
      ?.querySelectorAll<HTMLButtonElement>('button[aria-expanded="false"]')
      .forEach((button) => {
        button.click()
      })
  }, [view])
  return (
    <div ref={rootRef}>
      <SearchResponseResults view={view} />
    </div>
  )
}

const meta = {
  args: { view: groupedView },
  component: SearchResponseResults,
  parameters: { layout: 'padded' },
  tags: ['autodocs'],
  title: 'Components/QueryDetail/SearchResponseResults',
} satisfies Meta<typeof SearchResponseResults>

export default meta
type Story = StoryObj<typeof meta>

export const Grouped: Story = {}

export const GroupedExpanded: Story = {
  render: ({ view }) => <ExpandedResults view={view} />,
}

export const WithinLimit: Story = {
  args: { view: withinLimitView },
}

export const Empty: Story = {
  args: { view: { results: [], state: 'available' } },
}

export const Unavailable: Story = {
  args: { view: { state: 'unavailable' } },
}

export const TooLarge: Story = {
  args: { view: { state: 'tooLarge' } },
}
