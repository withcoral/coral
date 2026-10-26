use super::ConfigurationChoices;

// Catches: a configuration problem Coral finds in the source naming a field
// the Program never wrote, so it cannot tell what to correct.
#[test]
fn a_configuration_field_names_the_argument_to_correct() {
    for (configuration_field, argument) in [
        ("#/servers", "servers.#/servers"),
        ("display_name", "displayName"),
        ("request_headers.Host", "requestHeaders.Host"),
        ("credential_fields.token", "credentialFields.token"),
        ("authentication.fields.token", "credentialFields.token"),
        ("application", "authentication.client"),
        ("server", "servers"),
        ("authentication", "authentication"),
        // Only a whole segment is renamed.
        ("servers_extra", "servers_extra"),
    ] {
        assert_eq!(
            ConfigurationChoices::argument_for(configuration_field),
            argument,
            "{configuration_field}"
        );
    }
}
