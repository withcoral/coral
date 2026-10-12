//! Live DNS/TLS acceptance. The Reel container owns the sidecar and trust store.
#![cfg(feature = "transport")]
#![allow(
    unused_crate_dependencies,
    reason = "live transport test does not use every library dependency"
)]

use coral_outbound::{
    Client, DestinationPolicy,
    NetworkAccess::{Deny, HttpAndHttps, Https},
};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires the disposable Reel container sidecar and CA"]
async fn private_hostname_http_and_verified_https_preserve_security_controls() {
    let base = "http://sidecar:5000";
    let secure = "https://sidecar:5443";
    let untrusted = "https://sidecar:5444";
    for access in [Deny, Https, HttpAndHttps] {
        let client =
            Client::new(DestinationPolicy::new(Deny, access), Duration::from_secs(3)).unwrap();
        if access == HttpAndHttps {
            assert_eq!(
                client
                    .get(base)
                    .unwrap()
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap(),
                "sidecar"
            );
        } else {
            client.get(base).unwrap_err();
        }
        let result = client.get(secure).unwrap().send().await;
        if access == Deny {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().text().await.unwrap(), "sidecar");
        }
        client.get(untrusted).unwrap().send().await.unwrap_err();
        // A permitted HTTP category never allows public HTTP, metadata or loopback.
        for url in [
            "http://8.8.8.8/",
            "https://169.254.169.254/",
            "http://localhost:5000/",
        ] {
            client.get(url).unwrap_err();
        }
    }
}
