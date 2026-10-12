import { createContext } from 'react-router'

import type { RequestAuth } from './types'

export const requestAuthContext = createContext<RequestAuth>()
