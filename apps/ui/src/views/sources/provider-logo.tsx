import classNames from 'classnames'
import { useEffect, useState } from 'react'

import { Icon } from '@/wax/components/icon'

import { providerIcon, providerIconNeedsDarkInvert } from '@/lib/provider-icons'

import * as styles from './provider-logo.css'

type ProviderLogoSize = 'large' | 'medium' | 'small'

const FALLBACK_ICON_SIZE: Record<ProviderLogoSize, '16' | '18' | '20'> = {
  large: '20',
  medium: '18',
  small: '16',
}

const FALLBACK_ICON_COLOR: Record<ProviderLogoSize, 'secondary' | 'tertiary'> = {
  large: 'secondary',
  medium: 'tertiary',
  small: 'tertiary',
}

export function ProviderLogo({
  className,
  name,
  size = 'medium',
}: {
  className?: string
  name: string
  size?: ProviderLogoSize
}) {
  const icon = providerIcon(name)
  const [imageFailed, setImageFailed] = useState(false)

  useEffect(() => {
    setImageFailed(false)
  }, [icon])

  return (
    <span className={classNames(styles.root, styles.size[size], className)}>
      {icon && !imageFailed ? (
        <img
          alt=""
          className={classNames(styles.image, {
            [styles.imageInvertInDark]: providerIconNeedsDarkInvert(name),
          })}
          onError={() => setImageFailed(true)}
          src={icon}
        />
      ) : (
        <Icon color={FALLBACK_ICON_COLOR[size]} name="Plug" size={FALLBACK_ICON_SIZE[size]} />
      )}
    </span>
  )
}
