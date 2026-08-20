import { Outlet } from 'react-router'

// Each settings page draws its own frame, so this layer only holds the nesting
// the route tree needs.
export default function SettingsRoute() {
  return <Outlet />
}
