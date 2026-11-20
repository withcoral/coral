//! Signing matches AWS's published `SigV4` test vectors.

mod support;

use coral_aws::{Identity, Settings};
use support::{ACCOUNT, caller_identity, environment, request, vector_time};
use wiremock::MockServer;

const KEY: &str = "AKIDEXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const TOKEN: &str = "6e86291e8372ff2a2260956d9b8aae1d763fbf315fa00fa31553b73ebf194267";

struct Vector {
    name: &'static str,
    method: &'static str,
    path: &'static str,
    headers: &'static [(&'static str, &'static str)],
    body: &'static str,
    token: bool,
    signed_headers: &'static str,
    signature: &'static str,
}

/// From `aws-signing-test-suite/v4`, as `aws-sigv4` ships it.
const VECTORS: [Vector; 8] = [
    Vector {
        name: "get-vanilla",
        method: "GET",
        path: "/",
        headers: &[],
        body: "",
        token: false,
        signed_headers: "host;x-amz-date",
        signature: "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
    },
    Vector {
        name: "get-vanilla-query-order-key-case",
        method: "GET",
        path: "/?Param2=value2&Param1=value1",
        headers: &[],
        body: "",
        token: false,
        signed_headers: "host;x-amz-date",
        signature: "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500",
    },
    Vector {
        name: "get-vanilla-with-session-token",
        method: "GET",
        path: "/",
        headers: &[],
        body: "",
        token: true,
        signed_headers: "host;x-amz-date;x-amz-security-token",
        signature: "07ec1639c89043aa0e3e2de82b96708f198cceab042d4a97044c66dd9f74e7f8",
    },
    Vector {
        name: "get-header-value-trim",
        method: "GET",
        path: "/",
        headers: &[("My-Header1", " value1"), ("My-Header2", " \"a   b   c\"")],
        body: "",
        token: false,
        signed_headers: "host;my-header1;my-header2;x-amz-date",
        signature: "acc3ed3afb60bb290fc8d2dd0098b9911fcaa05412b367055dee359757a9c736",
    },
    Vector {
        name: "get-slash-dot-slash-normalized",
        method: "GET",
        path: "/./",
        headers: &[],
        body: "",
        token: false,
        signed_headers: "host;x-amz-date",
        signature: "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
    },
    Vector {
        name: "post-vanilla-query",
        method: "POST",
        path: "/?Param1=value1",
        headers: &[],
        body: "",
        token: false,
        signed_headers: "host;x-amz-date",
        signature: "28038455d6de14eafc1f9222cf5aa6f1a96197d7deb8263271d420d138af7f11",
    },
    Vector {
        name: "post-header-key-sort",
        method: "POST",
        path: "/",
        headers: &[("My-Header1", "value1")],
        body: "",
        token: false,
        signed_headers: "host;my-header1;x-amz-date",
        signature: "c5410059b04c1ee005303aed430f6e6645f61f4dc9e1461ec8f8916fdf18852c",
    },
    // The suite signs this body's hash as a header; a caller that wants it
    // sets the header itself.
    Vector {
        name: "post-x-www-form-urlencoded",
        method: "POST",
        path: "/",
        headers: &[
            ("Content-Type", "application/x-www-form-urlencoded"),
            ("Content-Length", "13"),
            (
                "x-amz-content-sha256",
                "9095672bbd1f56dfc5b65f3e153adc8731a4a654192329106275f4c7b24d0b6e",
            ),
        ],
        body: "Param1=value1",
        token: false,
        signed_headers: "content-length;content-type;host;x-amz-content-sha256;x-amz-date",
        signature: "d3875051da38690788ef43de4db0d8f280229d82040bfac253562e56c3f20e0b",
    },
];

#[tokio::test]
async fn signing_matches_the_published_sigv4_test_vectors() {
    let sts = MockServer::start().await;
    caller_identity(&sts, KEY, ACCOUNT).await;
    let settings = Settings::new(ACCOUNT, "us-east-1", None, None).unwrap();
    let with_token = Identity::new(
        settings.clone(),
        "vectors",
        &environment(
            &sts,
            &[
                ("AWS_ACCESS_KEY_ID", KEY),
                ("AWS_SECRET_ACCESS_KEY", SECRET),
                ("AWS_SESSION_TOKEN", TOKEN),
            ],
        ),
    )
    .unwrap();
    let without_token = Identity::new(
        settings,
        "vectors",
        &environment(
            &sts,
            &[
                ("AWS_ACCESS_KEY_ID", KEY),
                ("AWS_SECRET_ACCESS_KEY", SECRET),
            ],
        ),
    )
    .unwrap();

    for vector in VECTORS {
        let mut request = request(
            vector.method,
            &format!("https://example.amazonaws.com{}", vector.path),
        );
        for (name, value) in vector.headers {
            request.headers_mut().append(*name, value.parse().unwrap());
        }
        *request.body_mut() = vector.body.as_bytes().to_vec();
        let identity = if vector.token {
            &with_token
        } else {
            &without_token
        };

        identity
            .sign(&mut request, "service", "us-east-1", vector_time())
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", vector.name));

        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .map(|value| value.to_str().unwrap().to_owned())
        };
        assert_eq!(
            header("authorization").as_deref(),
            Some(
                format!(
                    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
                     SignedHeaders={}, Signature={}",
                    vector.signed_headers, vector.signature
                )
                .as_str()
            ),
            "{}",
            vector.name
        );
        assert_eq!(
            header("x-amz-date").as_deref(),
            Some("20150830T123600Z"),
            "{}",
            vector.name
        );
        assert_eq!(
            header("x-amz-security-token").as_deref(),
            vector.token.then_some(TOKEN),
            "{}",
            vector.name
        );
    }
}
