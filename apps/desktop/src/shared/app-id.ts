// The application identity, in two places that must agree. electron-builder
// stamps it on the bundle and the Windows shortcut, and the main process
// registers the same string at startup as the AppUserModelID and names its data
// folder after it. Windows treats two spellings of the id as two applications.
//
// It differs from the released 0.x app (`com.withcoral.desktop`), so this
// app keeps its own data folder and never reads that app's state. Both apps
// share an install location on macOS and Windows; see the Desktop README. Keep it permanently: a new
// id is a new app with an empty data folder.
export const APP_ID = 'com.withcoral.app'
