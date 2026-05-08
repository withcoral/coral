import { Shell } from '@/components/shell'
import { TracesPage } from '@/views/TracesPage'
import { useThemeClassOnBody } from '@/wax/theme/theme-provider'
import '@/app.css'

export function App() {
  useThemeClassOnBody()

  return (
    <Shell>
      <TracesPage />
    </Shell>
  )
}
