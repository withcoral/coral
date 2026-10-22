import classNames from 'classnames'
import ReactMarkdown, { type Components } from 'react-markdown'

import * as styles from './markdown.css'

const MARKDOWN_COMPONENTS: Components = {
  a: ({ node: _node, ...props }) => <a {...props} target="_blank" rel="noopener noreferrer" />,
  // Provider prose must not trigger requests to external image hosts.
  img: ({ alt }) => <span>{alt}</span>,
}

/** Compact Markdown typography. Links open in a new tab; HTML and remote images are not rendered. */
export function Markdown({
  children,
  color = 'secondary',
}: {
  children: string
  color?: keyof typeof styles.color
}) {
  return (
    <div className={classNames(styles.root, styles.color[color])}>
      <ReactMarkdown components={MARKDOWN_COMPONENTS}>{children}</ReactMarkdown>
    </div>
  )
}
