import { useEffect, useState } from 'react'
import { useNavigation } from 'react-router'

const DELAY_MS = 150 // Add some delay so we don't flash loading for quick navigations

export function useNavigationProgress(): boolean {
  const { state } = useNavigation()
  const [show, setShow] = useState(false)

  useEffect(() => {
    if (state === 'loading') {
      const timeout = setTimeout(() => setShow(true), DELAY_MS)
      return () => clearTimeout(timeout)
    }
    setShow(false)
  }, [state])

  return show
}
