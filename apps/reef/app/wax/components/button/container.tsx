import classNames from 'classnames'
import React, { ComponentProps, ElementType } from 'react'

import type { ButtonSize, ButtonVariant } from '@/wax/components/button/button.css'

import { button, disabledClass } from './button.css'
import {
  Icon,
  IconImplementation,
  LonelyIconImplementation,
  SpinningButtonIcon,
  SpinningIconImplementation,
} from './icon'
import { Text, TextImplementation } from './text'

export type ButtonProps<T extends ElementType = 'button'> = ButtonBaseProps &
  Omit<React.ComponentPropsWithoutRef<T>, 'as' | keyof ButtonBaseProps> & {
    as?: T
  }

interface ButtonBaseProps {
  ariaLabel?: string
  children?: React.ReactNode
  className?: string
  disabled?: boolean
  /**
   * Keeps a disabled button focusable and announced as disabled, so a tooltip
   * can say why. Its click runs but never submits a form.
   */
  focusableWhenDisabled?: boolean
  fullWidth?: boolean
  hasPrefix?: boolean
  hasSuffix?: boolean
  isActive?: boolean
  size?: ButtonSize
  variant?: ButtonVariant
}

export function Container<T extends ElementType = 'button'>(
  props: ButtonProps<T> & { ref?: React.Ref<HTMLElement> },
) {
  const {
    ariaLabel,
    as,
    children,
    className,
    disabled = false,
    focusableWhenDisabled = false,
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
  const type = 'type' in props ? props.type! : 'button'
  const childArray = React.Children.toArray(children)
  const isSymbolOnly = childArray.length === 1 && isButtonIcon(childArray[0])
  const hasPrefix = hasPrefixProp || (!isSymbolOnly && isButtonIcon(childArray[0]))
  const hasSuffix = hasSuffixProp || childArray.slice(1).some(isButtonIcon)

  const newChildren = childArray.flatMap<NonNullable<React.ReactNode>[]>(
    (child, index, childrenArray) => {
      if (!React.isValidElement(child)) {
        return [child]
      }
      const childElement = child as React.ReactElement<Record<string, unknown>>

      switch (child.type) {
        case Icon:
        case SpinningButtonIcon: {
          const iconProps = childElement.props as unknown as ComponentProps<typeof Icon>
          const IconComponent =
            child.type === SpinningButtonIcon ? SpinningIconImplementation : IconImplementation
          const LonelyIconComponent =
            child.type === SpinningButtonIcon
              ? SpinningIconImplementation
              : LonelyIconImplementation

          if (index === 0 && childrenArray.length === 1) {
            return [
              <LonelyIconComponent
                key={childElement.key}
                name={iconProps.name}
                size={size}
                variant={variant}
              />,
            ]
          }

          return [
            <IconComponent
              key={childElement.key}
              name={iconProps.name}
              size={size}
              variant={variant}
            />,
          ]
        }

        case Text: {
          const elements = [
            <TextImplementation
              buttonVariant={variant}
              key={child.key}
              size={size}
              {...childElement.props}
            />,
          ]

          return elements
        }

        default:
          return [child]
      }
    },
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
      className,
    ),
    ref,
    ...rest,
    // Only add button-specific props when rendering as button
    ...(Component === 'button' &&
      (disabled && focusableWhenDisabled
        ? {
            'aria-disabled': true,
            type,
            onClick: (event: React.MouseEvent<HTMLButtonElement>) => {
              event.preventDefault()
              ;(
                rest as { onClick?: (event: React.MouseEvent<HTMLButtonElement>) => void }
              ).onClick?.(event)
            },
          }
        : { disabled, type })),
    // For non-button elements, add aria-disabled and tabIndex for accessibility
    // Remove href for disabled anchors to prevent navigation
    ...(Component !== 'button' &&
      disabled && { 'aria-disabled': true, href: undefined, tabIndex: -1 }),
  }

  return <Component {...componentProps}>{newChildren}</Component>
}

function isButtonIcon(child: React.ReactNode): boolean {
  return React.isValidElement(child) && (child.type === Icon || child.type === SpinningButtonIcon)
}
