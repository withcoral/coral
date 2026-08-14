import { Field } from '@base-ui/react/field'
import classNames from 'classnames'

import { Container as ScrollArea } from '@/wax/components/scroll-area'

import * as styles from './text-area.css'

export interface TextAreaProps {
  ariaLabel?: string
  autoFocus?: boolean
  className?: string
  disabled?: boolean
  id?: string
  invalid?: boolean
  name?: string
  onBlur?: () => void
  onChange?: (value: string) => void
  onFocus?: () => void
  onKeyDown?: (event: React.KeyboardEvent<HTMLTextAreaElement>) => void
  placeholder?: string
  ref?: React.Ref<HTMLTextAreaElement>
  rows?: number
  value?: string
}

export function TextArea({
  ariaLabel,
  autoFocus,
  className,
  disabled,
  id,
  invalid,
  name,
  onBlur,
  onChange,
  onFocus,
  onKeyDown,
  placeholder,
  ref,
  rows = 3,
  value,
}: TextAreaProps) {
  return (
    <Field.Root disabled={disabled} invalid={invalid}>
      <ScrollArea
        className={styles.textAreaContainer}
        height="auto"
        scrollDirection="vertical"
        renderViewport={
          <Field.Control
            aria-label={ariaLabel}
            autoFocus={autoFocus}
            id={id}
            name={name}
            onBlur={onBlur}
            onFocus={onFocus}
            onValueChange={onChange}
            placeholder={placeholder}
            render={
              <textarea
                className={classNames(styles.textArea, className)}
                onKeyDown={onKeyDown}
                ref={ref}
                role="textbox"
                rows={rows}
                tabIndex={0}
              />
            }
            value={value}
          />
        }
      />
    </Field.Root>
  )
}
