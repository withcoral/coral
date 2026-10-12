import type { CSSProperties } from 'react'

import * as Table from '@/wax/components/table'
import { Typography } from '@/wax/components/typography'

import { Counts, EvidenceLink, Status, evidencePath, seconds } from './shared'
import type { ScenarioEntry } from './types'
import * as styles from './reel.css'

interface Folder {
  name: string
  path: string
  folders: Map<string, Folder>
  entries: ScenarioEntry[]
  descendants: ScenarioEntry[]
}

/** Source paths organize the inventory; scenario identities still own every link. */
export function ScenarioFolders({ entries, prefix }: { entries: ScenarioEntry[]; prefix: string }) {
  const root: Folder = { name: '', path: '', folders: new Map(), entries: [], descendants: [] }
  for (const entry of entries) {
    const parts = entry.result.source
      .replaceAll('\\', '/')
      .replace(/^e2e\/(?:scenarios\/)?/, '')
      .split('/')
      .slice(0, -1)
      .filter(Boolean)
    let folder = root
    for (const name of parts) {
      let child = folder.folders.get(name)
      if (!child) {
        child = {
          name,
          path: folder.path ? `${folder.path}/${name}` : name,
          folders: new Map(),
          entries: [],
          descendants: [],
        }
        folder.folders.set(name, child)
      }
      child.descendants.push(entry)
      folder = child
    }
    folder.entries.push(entry)
  }
  return <FolderRows folder={root} depth={-1} prefix={prefix} />
}

function FolderRows({ folder, depth, prefix }: { folder: Folder; depth: number; prefix: string }) {
  const rows = folder.entries.map(({ path, result }) => (
    <Table.Row key={path}>
      <Table.Cell wrap>
        <div
          className={styles.scenarioIndent}
          style={{ [styles.scenarioDepth]: depth + 1 } as CSSProperties}
        >
          <EvidenceLink
            wrap
            data-testid="scenario-link"
            href={`${prefix}${evidencePath(path)}/index.html`}
            title={result.source}
          >
            {result.scenario}
          </EvidenceLink>
        </div>
      </Table.Cell>
      <Table.Cell>
        <Status status={result.status} />
      </Table.Cell>
      <Table.Cell wrap>
        <Typography.BodySmall>{result.target}</Typography.BodySmall>
      </Table.Cell>
      <Table.Cell>
        <Typography.CodeSmallInline className={styles.duration}>
          {seconds(result.durationMs)}
        </Typography.CodeSmallInline>
      </Table.Cell>
    </Table.Row>
  ))
  return (
    <>
      {folder.path ? (
        <Table.Group
          label={folder.path}
          heading={
            <Table.Cell fullWidth wrap>
              <div
                className={`${styles.scenarioIndent} ${styles.sectionHeading}`}
                style={{ [styles.scenarioDepth]: depth } as CSSProperties}
              >
                <Typography.BodySmallStrong>{folder.name}/</Typography.BodySmallStrong>
                <Counts results={folder.descendants} />
              </div>
            </Table.Cell>
          }
        >
          {rows}
        </Table.Group>
      ) : (
        rows.length > 0 && <Table.Body>{rows}</Table.Body>
      )}
      {[...folder.folders.values()]
        .toSorted((a, b) => a.name.localeCompare(b.name))
        .map((child) => (
          <FolderRows key={child.path} folder={child} depth={depth + 1} prefix={prefix} />
        ))}
    </>
  )
}
