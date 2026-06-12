import ReactMarkdown, { type Components } from 'react-markdown'

import * as styles from './markdown.css'

const MARKDOWN_COMPONENTS: Components = {
  a: ({ node: _node, ...props }) => <a {...props} target="_blank" rel="noopener noreferrer" />,
}

/** Renders the subset of markdown that source-manifest hints use:
 *  links, inline code, bold/italic, and ordered/unordered lists.
 *  Links open in a new tab. Block elements (paragraphs, lists, code)
 *  pick up tighter spacing than the chat-message viewer. */
export function Markdown({ children }: { children: string }) {
  return (
    <div className={styles.root}>
      <ReactMarkdown components={MARKDOWN_COMPONENTS}>{children}</ReactMarkdown>
    </div>
  )
}
