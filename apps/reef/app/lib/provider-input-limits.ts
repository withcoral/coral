/** Mirrors coral_catalog::OPENAPI_DOCUMENT_MAX_BYTES for upload validation and copy. */
export const OPENAPI_DOCUMENT_MAX_BYTES = 64 * 1024 * 1024
export const OPENAPI_DOCUMENT_SIZE_LABEL = `${OPENAPI_DOCUMENT_MAX_BYTES / (1024 * 1024)} MiB`

/** Mirrors coral_provider::MAX_PROVIDER_ICON_BYTES for upload validation and copy. */
export const PROVIDER_ICON_MAX_BYTES = 100 * 1024
export const PROVIDER_ICON_SIZE_LABEL = `${PROVIDER_ICON_MAX_BYTES / 1024} KiB`
/** The image types Coral accepts as a provider icon; it reads the type from the file's bytes. */
export const PROVIDER_ICON_ACCEPT = 'image/png,image/jpeg,image/webp,image/svg+xml'
export const PROVIDER_ICON_RULE = `Use a PNG, JPEG, WebP or SVG file of ${PROVIDER_ICON_SIZE_LABEL} or less.`
