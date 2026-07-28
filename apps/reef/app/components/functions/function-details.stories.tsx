import type { Meta, StoryObj } from '@storybook/react-vite'

import { FunctionDetails } from './function-details'

const meta = {
  args: {
    arguments: [
      { dataType: 'Utf8', name: 'owner' },
      { dataType: 'Utf8', name: 'repo' },
    ],
    body: `select
  pull.number,
  pull.title,
  pull.html_url,
  pull.author_login
from github.pulls(owner => $owner, repo => $repo) as pull
where pull.state = 'open'
  and pull.review_decision = 'REVIEW_REQUIRED'
order by pull.updated_at desc`,
    description: 'Pull requests waiting for review in a repository.',
    name: 'github_review_queue',
    resultColumns: [
      { dataType: 'Int64', name: 'number', nullable: false },
      { dataType: 'Utf8', name: 'title', nullable: false },
      { dataType: 'Utf8', name: 'html_url', nullable: false },
      { dataType: 'Utf8', name: 'author_login', nullable: true },
    ],
    sources: ['github'],
  },
  component: FunctionDetails,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  title: 'Components/Functions/FunctionDetails',
} satisfies Meta<typeof FunctionDetails>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
