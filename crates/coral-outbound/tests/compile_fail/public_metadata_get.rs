use coral_outbound::{EndpointUrl, PublicMetadata};

fn fetch(url: &EndpointUrl<PublicMetadata>, http: &reqwest::Client) {
    url.get(http);
}

fn main() {}
