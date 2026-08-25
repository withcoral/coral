// The application identity, in two places that must agree. electron-builder
// stamps it on the Windows shortcut as the AppUserModelID, and the main process
// registers the same string at startup. Windows keys toasts, taskbar grouping,
// and jump lists off that id, and treats two spellings of it as two separate
// applications: the running window gets its own taskbar button instead of
// grouping under the pinned shortcut, and toasts from an unregistered id are
// dropped. macOS and Linux read it as the bundle id and the reverse-DNS name.
export const APP_ID = 'com.withcoral.desktop'
