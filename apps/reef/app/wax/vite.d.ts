import type { Plugin } from 'vite'

/** Each specifier that Wax imports at run time. */
export declare const runtimeImports: string[]

/** The Vite settings that an app needs to build Wax from source. */
export declare function wax(): Plugin
