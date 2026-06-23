import { Shell } from '@/components/shell'
import { ToastContainer } from '@/wax/components/toast'
import { useThemeClassOnBody } from '@/wax/theme/theme-provider'
import { Outlet, useNavigation } from 'react-router'
import '@/app.css'

export function App() {
  useThemeClassOnBody()
  const navigation = useNavigation()

  return (
    <Shell isNavigating={navigation.state !== 'idle'}>
      <Outlet />
      <ToastContainer />
    </Shell>
  )
}
