use coral_outbound::{Discovered, EndpointUrl};

fn fetch(url: &EndpointUrl<Discovered>, http: &reqwest::Client) {
    url.get(http);
}

fn main() {}
