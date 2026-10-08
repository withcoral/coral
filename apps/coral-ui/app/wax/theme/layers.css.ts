import { globalLayer } from '@vanilla-extract/css'

// Declaration order is layer order, and later layers win. App code outside app/wax
// stays unlayered, so it beats both of these whatever its specificity.
// app/styles/globals.css states the same order, because it is the first stylesheet
// the app loads. Keep the two in step.
//
// Element and pseudo element defaults: the reset in app/styles/globals.css, the page
// background, the selection colour, the scrollbars. `button { padding: 0 }` is a
// weaker selector than any wax rule but it is unlayered, and an unlayered rule beats
// every layer whatever its specificity, so leaving the reset out of a layer strips
// wax components of everything the reset names. It goes in the first layer.
export const resetLayer = globalLayer('reset')

// Third party stylesheets, currently react-toastify. They are unlayered as shipped,
// which puts them above every wax rule, so app/wax/components/toast/toastify.css
// imports them into this layer instead. Nothing writes to it from TypeScript; the
// name is declared here so that the order is stated in one place.
export const vendorLayer = globalLayer('vendor')

// Wax component styles. Wax rules and consumer overrides are both single class
// selectors, so which one wins used to depend on the order the bundler linked the
// CSS chunks, and dev and production disagreed. A layer settles it by construction.
export const waxLayer = globalLayer('wax')
