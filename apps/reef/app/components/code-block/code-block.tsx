import { Fragment, type ReactNode } from 'react'
import classNames from 'classnames'
import Prism from 'prismjs'
import 'prismjs/components/prism-sql'

import * as styles from './code-block.css'

const grammars = {
  sql: Prism.languages.sql,
}

export type CodeLanguage = keyof typeof grammars

export interface HighlightedCodeProps {
  className?: string
  code: string
  language: CodeLanguage
}

function renderToken(token: Prism.TokenStream, key: string): ReactNode {
  if (typeof token === 'string') {
    return <Fragment key={key}>{token}</Fragment>
  }

  if (Array.isArray(token)) {
    return token.map((child, index) => renderToken(child, `${key}.${index}`))
  }

  return (
    <span className={classNames('token', token.type, token.alias)} key={key}>
      {renderToken(token.content, `${key}.content`)}
    </span>
  )
}

export function HighlightedCode({ className, code, language }: HighlightedCodeProps) {
  return (
    <code className={classNames(styles.code, className)}>
      {renderToken(Prism.tokenize(code, grammars[language]), 'root')}
    </code>
  )
}

export function CodeBlock({ className, code, language }: HighlightedCodeProps) {
  return (
    <pre className={classNames(styles.block, className)}>
      <HighlightedCode code={code} language={language} />
    </pre>
  )
}
