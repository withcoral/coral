use coral_outbound::{ClientMetadata, EndpointUrl};

fn fetch(url: &EndpointUrl<ClientMetadata>, http: &reqwest::Client) {
    url.post(http);
}

fn main() {}
