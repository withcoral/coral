// scripts/build.mjs inlines SVG files as text.
declare module '*.svg' {
  const markup: string
  export default markup
}
