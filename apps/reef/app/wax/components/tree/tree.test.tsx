import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'

import { filterQuery } from '@/utils/filter-query'

import { Tree, filterNodes, type TreeNode } from './tree'

const nodes: TreeNode[] = [
  {
    id: 'group:logs',
    label: 'logs',
    children: [
      { id: 'logs.list', label: 'logs.list', searchTerms: ['logs.list', 'Read the log'] },
      { id: 'logs.send', label: 'logs.send', searchTerms: ['logs.send', 'Write a log'] },
    ],
  },
  {
    id: 'group:metrics',
    label: 'metrics',
    children: [{ id: 'metrics.read', label: 'metrics.read', searchTerms: ['metrics.read'] }],
  },
]

describe('filterNodes', () => {
  it('returns every node when nothing is searched for', () => {
    expect(filterNodes(nodes, filterQuery(''))).toBe(nodes)
  })

  it('keeps a branch only for the children that match', () => {
    const [group, ...rest] = filterNodes(nodes, filterQuery('logs.send'))
    expect(rest).toEqual([])
    expect(group.id).toBe('group:logs')
    expect(group.children?.map((child) => child.id)).toEqual(['logs.send'])
  })

  it('matches a leaf on its label when it declares no search terms', () => {
    expect(
      filterNodes([{ id: 'one', label: 'Solitary' }], filterQuery('solit')).map((n) => n.id),
    ).toEqual(['one'])
  })

  it('drops a branch whose children all fail to match', () => {
    expect(filterNodes(nodes, filterQuery('nothing-matches-this'))).toEqual([])
  })
})

describe('Tree expansion', () => {
  it('owns its expansion when the page passes none', () => {
    const html = renderToStaticMarkup(
      <Tree label="Tools" nodes={nodes} defaultCollapsedIds={['group:logs']} />,
    )
    expect(html).not.toContain('logs.list')
    expect(html).toContain('metrics.read')
  })

  it('shows exactly what a controlling page says is collapsed', () => {
    const html = renderToStaticMarkup(
      <Tree label="Tools" nodes={nodes} collapsedIds={['group:metrics']} />,
    )
    expect(html).toContain('logs.list')
    expect(html).not.toContain('metrics.read')
  })

  it('treats an empty controlled list as everything open', () => {
    const html = renderToStaticMarkup(
      <Tree label="Tools" nodes={nodes} collapsedIds={[]} defaultCollapsedIds={['group:logs']} />,
    )
    expect(html).toContain('logs.list')
    expect(html).toContain('metrics.read')
  })
})
