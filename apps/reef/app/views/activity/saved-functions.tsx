import { Link } from 'react-router'
import * as Card from '@/wax/components/card'
import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'
import { offset } from './duration'
import { rowLabel } from './record'
import * as styles from './saved-functions.css'
import type { SavedFunction } from './trace-summary'

/**
 * The functions a root saved, one card each: its name, linked to Functions, when a later Run
 * called it by its path; otherwise the step that saved it. Each save links to its record, the
 * first as "saved", each later one as "revised". "Smoke-tested" means a Run that succeeded
 * called the function after its last save.
 */
export function SavedFunctions({
  functions,
  workspace,
  origin,
  href,
  selected,
}: {
  functions: SavedFunction[]
  workspace: string
  /** The root's start, which each save's time counts from. */
  origin: number
  href: (record: string) => string
  selected: string | null
}) {
  if (!functions.length) return null
  const since = (row: { startedAt: string }) => offset(Date.parse(row.startedAt) - origin)
  return (
    <section aria-label="Saved functions" className={styles.outputs}>
      <Typography.BodySmallStrong>Saved functions</Typography.BodySmallStrong>
      <Card.List className={styles.cards}>
        {functions.map(({ key, path, saves, tested }) => {
          const [first, ...later] = saves
          // A function reads by its own name; its full path shows on hover.
          const name = path?.split('.').at(-1) ?? rowLabel(first!)
          return (
            <Card.Item key={key}>
              <div
                className={styles.card}
                aria-current={saves.some((row) => row.name === selected) ? 'true' : undefined}
              >
                <Icon name="Save" size="14" color="secondary" />
                <span className={styles.cardBody}>
                  <Link
                    to={
                      path
                        ? `/${workspace}/functions/${encodeURIComponent(path)}`
                        : href(first!.name)
                    }
                    title={path}
                    className={styles.cardName}
                  >
                    <Typography.BodySmallStrong color="primary">{name}</Typography.BodySmallStrong>
                    {path && <Icon name="ExternalLink" size="14" color="tertiary" />}
                  </Link>
                  <Typography.BodySmall>
                    <Link to={href(first!.name)} preventScrollReset>
                      saved {since(first!)}
                    </Link>
                    {later.map((row) => (
                      <span key={row.name}>
                        {' · '}
                        <Link to={href(row.name)} preventScrollReset>
                          revised {since(row)}
                        </Link>
                      </span>
                    ))}
                    {tested && (
                      <>
                        {' · '}
                        <span className={styles.tested}>
                          smoke-tested
                          <Icon name="Check" size="14" color="success" />
                        </span>
                      </>
                    )}
                  </Typography.BodySmall>
                </span>
              </div>
            </Card.Item>
          )
        })}
      </Card.List>
    </section>
  )
}
