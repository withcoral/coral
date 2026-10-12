import { useEffect, useState } from 'react'
import { useNavigation } from 'react-router'

const DELAY_MS = 150 // Add some delay so we don't flash loading for quick navigations

export function useNavigationProgress(): boolean {
  const navigation = useNavigation()
  const loadingLocation = navigation.state === 'loading' ? navigation.location : null
  const [visibleLocation, setVisibleLocation] = useState<typeof navigation.location | null>(null)
  const [previousLoadingLocation, setPreviousLoadingLocation] = useState(loadingLocation)

  if (loadingLocation !== previousLoadingLocation) {
    setPreviousLoadingLocation(loadingLocation)
    setVisibleLocation(null)
  }

  useEffect(() => {
    if (loadingLocation !== null) {
      const timeout = setTimeout(() => setVisibleLocation(loadingLocation), DELAY_MS)
      return () => clearTimeout(timeout)
    }
  }, [loadingLocation])

  return loadingLocation !== null && visibleLocation === loadingLocation
}
