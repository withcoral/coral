import classNames from 'classnames'
import React, { ComponentProps, ElementType, MouseEvent } from 'react'

import type { ButtonSize, ButtonVariant } from '@/wax/components/button/button.css'

import { button, disabledClass } from './button.css'
import { Icon, IconImplementation, LonelyIconImplementation } from './icon'
import { Text, TextImplementation } from './text'

export type ButtonProps<T extends ElementType = 'button'> = ButtonBaseProps &
  Omit<React.ComponentPropsWithoutRef<T>, 'as' | keyof ButtonBaseProps> & {
    as?: T
  }

function handleDisabledClick(event: MouseEvent<HTMLElement>) {
  event.preventDefault()
  event.stopPropagation()
}

interface ButtonBaseProps {
  ariaLabel?: string
  children?: React.ReactNode
  className?: string
  disabled?: boolean
  fullWidth?: boolean
  hasPrefix?: boolean
  hasSuffix?: boolean
  isActive?: boolean
  size?: ButtonSize
  variant?: ButtonVariant
}

export function Container<T extends ElementType = 'button'>(props: ButtonProps<T> & { ref?: React.Ref<HTMLElement> }) {
  const {
    ariaLabel,
    as,
    children,
    className,
    disabled = false,
    fullWidth = false,
    hasPrefix: hasPrefixProp = false,
    hasSuffix: hasSuffixProp = false,
    isActive = false,
    ref,
    size = '32',
    variant = 'primary',
    ...rest
  } = props

  const Component = (as ?? 'button') as ElementType
  const isNativeButton = Component === 'button'
  const type = 'type' in props ? props.type! : 'button'
  let hasPrefix = hasPrefixProp
  let hasSuffix = hasSuffixProp
  let isSymbolOnly = false

  const newChildren = React.Children.toArray(children).flatMap<NonNullable<React.ReactNode>[]>(
    (child, index, childrenArray) => {
      if (!React.isValidElement(child)) {
        return [child]
      }
      const childElement = child as React.ReactElement<Record<string, unknown>>

      switch (child.type) {
        case Icon: {
          const iconProps = childElement.props as unknown as ComponentProps<typeof Icon>

          if (index === 0 && childrenArray.length === 1) {
            isSymbolOnly = true

            return [
              <LonelyIconImplementation key={childElement.key} name={iconProps.name} size={size} variant={variant} />,
            ]
          }

          if (index === 0) {
            hasPrefix = true
          } else {
            hasSuffix = true
          }

          return [<IconImplementation key={childElement.key} name={iconProps.name} size={size} variant={variant} />]
        }

        case Text: {
          return [
            <TextImplementation buttonVariant={variant} key={child.key} size={size} {...childElement.props} />,
          ]
        }

        default:
          return [child]
      }
    }
  )

  const componentProps = {
    'aria-label': ariaLabel,
    className: classNames(
      button({
        disabled,
        fullWidth,
        hasPrefix,
        hasSuffix,
        isActive,
        isSymbolOnly,
        size,
        variant,
      }),
      { [disabledClass]: disabled },
      className
    ),
    ref,
    ...rest,
    onClick: !isNativeButton && disabled ? handleDisabledClick : rest.onClick,
    ...(isNativeButton && { disabled, type }),
    ...(!isNativeButton && disabled && { 'aria-disabled': true, href: undefined, tabIndex: -1 }),
  }

  return <Component {...componentProps}>{newChildren}</Component>
}
