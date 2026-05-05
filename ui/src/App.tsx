import { Shell } from '@/components/shell'
import { Placeholder } from '@/views/Placeholder'
import { useThemeClassOnBody } from '@/wax/theme/theme-provider'
import '@/app.css'

export function App() {
  useThemeClassOnBody()

  return (
    <Shell>
      <Placeholder />
    </Shell>
  )
}
