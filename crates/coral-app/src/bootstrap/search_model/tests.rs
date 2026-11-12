//! The delivery of the search model's files, through [`SearchModel`], with
//! a test manifest whose files are a few bytes, served by a local HTTP
//! server through a real outbound client.

use std::path::{Path, PathBuf};
use std::time::Duration;

use coral_embed::{MANIFEST, Manifest, ModelFile};
use coral_outbound::{DestinationPolicy, NetworkAccess};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{Delivery, RetryTiming, SearchModel, SearchModelState};

const MODEL: &[u8] = b"model bytes";
const TOKENIZER: &[u8] = b"tokenizer bytes";
const MODEL_ID: &str = "test-model";
const RELEASE_PATH: &str = "/release";

/// How long a test waits for the module before it fails.
const PATIENCE: Duration = Duration::from_secs(10);

/// [`MANIFEST`] with files of a few bytes, released at `server`.
fn manifest(server: &MockServer) -> Manifest {
    Manifest {
        model_id: MODEL_ID,
        download_url: format!("{}{RELEASE_PATH}", server.uri()).leak(),
        model_file: ModelFile {
            name: "model.onnx",
            size: 11,
            sha256: "9cb7487000bc86ac36ce83c4acfabe8878552be99572a6770f65ab1d048a5c48",
        },
        tokenizer_file: ModelFile {
            name: "tokenizer.json",
            size: 15,
            sha256: "6ea0af464efe55bcdcd55fa91cde0e8b0e9647e59273110caee85c96de037652",
        },
        ..MANIFEST
    }
}

/// The folders of one test: a read-only folder looked in first, like the
/// system path, and the state directory's folder that downloads go to.
struct Dirs {
    _temp: TempDir,
    system: PathBuf,
    state: PathBuf,
}

impl Dirs {
    fn new() -> Self {
        let temp = TempDir::new().expect("temp dir");
        Self {
            system: temp.path().join("system"),
            state: temp.path().join("state"),
            _temp: temp,
        }
    }

    /// The model folder that downloads go to.
    fn downloaded(&self) -> PathBuf {
        self.state.join(MODEL_ID)
    }
}

fn start(server: &MockServer, dirs: &Dirs, loopback: NetworkAccess) -> SearchModel {
    let client = coral_outbound::Client::new(
        DestinationPolicy::new(loopback, NetworkAccess::Deny),
        // Longer than every test waits, so only the stall bound ends a
        // silent server.
        Duration::from_mins(1),
    )
    .expect("client");
    let (model, _delivery) = SearchModel::start(Delivery {
        manifest: manifest(server),
        models_dirs: vec![dirs.system.clone(), dirs.state.clone()],
        download_dir: dirs.state.clone(),
        client,
        retry: RetryTiming {
            first: Duration::from_millis(10),
            max: Duration::from_millis(50),
            stall: Duration::from_millis(500),
        },
    });
    model
}

/// Serves `body` at the release's `file`.
async fn serve(server: &MockServer, file: &str, body: &[u8]) {
    Mock::given(method("GET"))
        .and(path(format!("{RELEASE_PATH}/{file}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .mount(server)
        .await;
}

async fn ready(model: &SearchModel) -> Option<PathBuf> {
    tokio::time::timeout(PATIENCE, model.ready())
        .await
        .expect("the model is ready in time")
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

#[tokio::test]
async fn absent_files_are_downloaded_checked_and_reported_ready() {
    let server = MockServer::start().await;
    serve(&server, "model.onnx", MODEL).await;
    serve(&server, "tokenizer.json", TOKENIZER).await;
    let dirs = Dirs::new();

    let model = start(&server, &dirs, NetworkAccess::HttpAndHttps);

    assert_eq!(ready(&model).await, Some(dirs.downloaded()));
    assert_eq!(read(&dirs.downloaded().join("model.onnx")), MODEL);
    assert_eq!(read(&dirs.downloaded().join("tokenizer.json")), TOKENIZER);
}

/// Waits until `server` has answered `count` requests for the release's
/// `file`.
async fn requested(server: &MockServer, file: &str, count: usize) {
    let path = format!("{RELEASE_PATH}/{file}");
    tokio::time::timeout(PATIENCE, async {
        loop {
            let requests = server.received_requests().await.unwrap_or_default();
            if requests
                .iter()
                .filter(|request| request.url.path() == path)
                .count()
                >= count
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_elapsed| panic!("{count} requests for {file} in time"));
}

#[tokio::test]
async fn a_file_with_another_digest_never_gets_the_final_name_and_is_downloaded_again() {
    let server = MockServer::start().await;
    serve(&server, "model.onnx", b"model bytez").await;
    serve(&server, "tokenizer.json", TOKENIZER).await;
    let dirs = Dirs::new();

    let model = start(&server, &dirs, NetworkAccess::HttpAndHttps);
    // The second request starts only after the first attempt has ended.
    requested(&server, "model.onnx", 2).await;

    assert!(
        !dirs.downloaded().join("model.onnx").exists(),
        "a file with another digest got the final name"
    );
    assert_eq!(model.state(), SearchModelState::Downloading);
    assert_eq!(model.looked().await, Some(SearchModelState::Downloading));
}

/// Answers a request for the release's `file` with a redirect to
/// `location`, as GitHub sends a release download to its asset host.
async fn redirect(server: &MockServer, file: &str, location: &str) {
    Mock::given(method("GET"))
        .and(path(format!("{RELEASE_PATH}/{file}")))
        .respond_with(ResponseTemplate::new(302).insert_header("location", location))
        .mount(server)
        .await;
}

/// Serves `body` at `asset_path`, outside the release.
async fn serve_asset(server: &MockServer, asset_path: &str, body: &[u8]) {
    Mock::given(method("GET"))
        .and(path(asset_path))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .mount(server)
        .await;
}

#[tokio::test]
async fn a_download_follows_the_release_redirects_to_the_asset_host() {
    let server = MockServer::start().await;
    redirect(
        &server,
        "model.onnx",
        &format!("{}/assets/1?signature=x", server.uri()),
    )
    .await;
    redirect(&server, "tokenizer.json", "/assets/2").await;
    serve_asset(&server, "/assets/1", MODEL).await;
    serve_asset(&server, "/assets/2", TOKENIZER).await;
    let dirs = Dirs::new();

    let model = start(&server, &dirs, NetworkAccess::HttpAndHttps);

    assert_eq!(ready(&model).await, Some(dirs.downloaded()));
    assert_eq!(read(&dirs.downloaded().join("model.onnx")), MODEL);
}

/// A local address where nothing listens.
fn closed_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("address");
    drop(listener);
    format!("http://{address}/model.onnx")
}

#[tokio::test]
async fn a_failed_attempt_is_retried_until_one_succeeds() {
    let cases = [
        (
            "connection refused",
            ResponseTemplate::new(302).insert_header("location", closed_address()),
        ),
        ("server error", ResponseTemplate::new(500)),
        ("not found", ResponseTemplate::new(404)),
        (
            "stalled server",
            ResponseTemplate::new(200)
                .set_body_bytes(MODEL)
                .set_delay(Duration::from_secs(30)),
        ),
        ("redirect without a location", ResponseTemplate::new(302)),
    ];
    for (case, failure) in cases {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("{RELEASE_PATH}/model.onnx")))
            .respond_with(failure)
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        serve(&server, "model.onnx", MODEL).await;
        serve(&server, "tokenizer.json", TOKENIZER).await;
        let dirs = Dirs::new();

        let model = start(&server, &dirs, NetworkAccess::HttpAndHttps);

        assert_eq!(
            tokio::time::timeout(PATIENCE, model.ready()).await,
            Ok(Some(dirs.downloaded())),
            "{case}"
        );
        assert_eq!(read(&dirs.downloaded().join("model.onnx")), MODEL, "{case}");
    }
}

/// Puts the test manifest's files in `models_dir`.
fn place_files(models_dir: &Path) {
    let dir = models_dir.join(MODEL_ID);
    std::fs::create_dir_all(&dir).expect("create the model folder");
    std::fs::write(dir.join("model.onnx"), MODEL).expect("write the model");
    std::fs::write(dir.join("tokenizer.json"), TOKENIZER).expect("write the tokenizer");
}

/// Puts the folder of another model, with a file, in `models_dir`.
fn place_other_model(models_dir: &Path) -> PathBuf {
    let dir = models_dir.join("old-model");
    std::fs::create_dir_all(&dir).expect("create the old model folder");
    std::fs::write(dir.join("model.onnx"), b"old").expect("write the old model");
    dir
}

#[tokio::test]
async fn files_on_disk_are_ready_without_a_request() {
    for (case, in_system) in [("system folder", true), ("state folder", false)] {
        let server = MockServer::start().await;
        let dirs = Dirs::new();
        place_files(if in_system { &dirs.system } else { &dirs.state });
        let other_model = place_other_model(&dirs.state);

        let model = start(&server, &dirs, NetworkAccess::HttpAndHttps);

        let expected = if in_system {
            dirs.system.join(MODEL_ID)
        } else {
            dirs.downloaded()
        };
        assert_eq!(ready(&model).await, Some(expected.clone()), "{case}");
        assert_eq!(
            model.looked().await,
            Some(SearchModelState::Ready(expected)),
            "{case}"
        );
        assert_eq!(
            server.received_requests().await.unwrap_or_default().len(),
            0,
            "{case}"
        );
        assert!(!other_model.exists(), "{case}: the other model stays");
    }
}

#[tokio::test]
async fn other_model_folders_are_removed_once_the_model_is_ready_and_not_before() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let dirs = Dirs::new();
    let other_model = place_other_model(&dirs.state);

    let model = start(&server, &dirs, NetworkAccess::HttpAndHttps);
    requested(&server, "tokenizer.json", 2).await;

    assert!(other_model.exists(), "removed before the model is ready");

    server.reset().await;
    serve(&server, "model.onnx", MODEL).await;
    serve(&server, "tokenizer.json", TOKENIZER).await;
    assert_eq!(ready(&model).await, Some(dirs.downloaded()));
    assert!(!other_model.exists(), "kept after the model is ready");
}

#[tokio::test]
async fn the_destination_policy_decides_whether_the_files_are_downloaded() {
    let server = MockServer::start().await;
    serve(&server, "model.onnx", MODEL).await;
    serve(&server, "tokenizer.json", TOKENIZER).await;
    let dirs = Dirs::new();

    let model = start(&server, &dirs, NetworkAccess::Deny);
    // Many attempts fit in this time: the first retry waits 10 ms.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(model.state(), SearchModelState::Downloading);
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
    );
    assert!(!dirs.downloaded().join("tokenizer.json").exists());
}
