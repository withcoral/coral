// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! Coral's metadata document: what an Identity Provider needs to know of
//! Coral, at the URL that is Coral's entity ID.

use crate::xml;

/// The media type of a SAML metadata document.
pub(crate) const MEDIA_TYPE: &str = "application/samlmetadata+xml";

/// The metadata document of Coral at the entity ID `entity_id`, whose URL
/// `acs_url` receives the response. It holds no key, no `NameIDFormat`, no
/// logout service, no signature and no end date. `WantAssertionsSigned`
/// states a wish, not a rule.
pub(crate) fn document(entity_id: &str, acs_url: &str) -> String {
    format!(
        "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" \
         entityID=\"{entity_id}\">\
         <md:SPSSODescriptor \
         protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\" \
         AuthnRequestsSigned=\"false\" WantAssertionsSigned=\"true\">\
         <md:AssertionConsumerService \
         Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" \
         Location=\"{acs_url}\" index=\"0\" isDefault=\"true\"/>\
         </md:SPSSODescriptor>\
         </md:EntityDescriptor>",
        entity_id = xml::escape(entity_id),
        acs_url = xml::escape(acs_url),
    )
}
