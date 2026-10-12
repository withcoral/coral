import classNames from 'classnames'
import { useMemo } from 'react'
import { CopyButton } from '@/wax/components/button/copy-button'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Typography } from '@/wax/components/typography'
import { formatJson } from './format-json'
import { highlight, type Language } from './highlight'
import * as styles from './code-block.css'

/**
 * Exact read-only evidence. JSON formatting preserves the original value tokens.
 * JSON and TypeScript are syntax highlighted on the text colour, so no token
 * colour doubles as the base. Plain text keeps the code colour.
 * A framed block stands apart on its own surface, in the text colour, and
 * scrolls sideways rather than wrapping, so each line stays one line. With
 * `wrap` it wraps instead, for one long line that must stay in view.
 * A block with `maxLines` grows to that many lines, then scrolls inside itself
 * under a fade, so a long value stays open without taking the page.
 * A block with `copy` is framed and holds its copy button in the corner, beside
 * the code rather than over it, so scrolled code never runs under the button.
 */
export function CodeBlock({
  value,
  format = 'text',
  framed: framedProp = false,
  wrap = false,
  maxLines,
  copy,
}: {
  value: string
  format?: 'text' | Language
  framed?: boolean
  wrap?: boolean
  maxLines?: number
  /** What the copy button copies, which may differ from the shown value, and its accessible name. */
  copy?: { text: string; label: string }
}) {
  const display = useMemo(() => {
    if (format === 'text') return value
    const source = format === 'json' ? formatJson(value) : value
    return highlight(source, format).map(({ text, className }, index) =>
      className ? (
        <span key={index} className={className}>
          {text}
        </span>
      ) : (
        text
      ),
    )
  }, [format, value])
  const framed = framedProp || Boolean(copy)
  const variant = framed || format !== 'text' ? 'primary' : undefined
  if (maxLines !== undefined || copy) {
    const scroll = (
      <ScrollArea
        className={classNames(styles.capped, {
          [styles.cappedFrame]: framed && !copy,
        })}
        maxHeight={
          maxLines === undefined
            ? 'none'
            : `calc(${maxLines}lh + ${framed ? styles.FRAME_PADDING * 2 : 0}px)`
        }
        fade={maxLines === undefined ? 'none' : 'bottom'}
        horizontal={framed && !wrap}
        renderViewport={
          <Typography.Code
            as="pre"
            variant={variant}
            className={classNames(styles.code, {
              [styles.cappedFramedCode]: framed,
              [styles.framedWrap]: framed && wrap,
            })}
          >
            {display}
          </Typography.Code>
        }
      />
    )
    if (!copy) return scroll
    return (
      <div className={styles.copyFrame}>
        {scroll}
        <CopyButton
          className={styles.copyButton}
          ariaLabel={copy.label}
          tooltipText={copy.label}
          textToCopy={copy.text}
          variant="bare"
        />
      </div>
    )
  }
  return (
    <Typography.Code
      as="pre"
      variant={variant}
      className={classNames(styles.code, {
        [styles.framed]: framed,
        [styles.framedWrap]: framed && wrap,
      })}
    >
      {display}
    </Typography.Code>
  )
}
