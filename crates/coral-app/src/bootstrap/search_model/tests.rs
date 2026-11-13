//! The delivery of the search model's files, through [`SearchModel`], with
//! a test manifest whose files are a few bytes, served by a local HTTP
//! server through a real outbound client.

use std::path::{Path, PathBuf};
use std::time::Duration;

use coral_embed::{CpuTooOld, MANIFEST, Manifest, ModelFile};
use coral_outbound::{DestinationPolicy, NetworkAccess};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    Delivery, DownloadError, DownloadTiming, SearchModel, SearchModelState, SearchModelStop,
    download,
};

const MODEL: &[u8] = b"model bytes";
const TOKENIZER: &[u8] = b"tokenizer bytes";
const MODEL_ID: &str = "test-model";
const RELEASE_PATH: &str = "/release";

/// How long a test waits for the module before it fails.
const PATIENCE: Duration = Duration::from_secs(10);

/// Waits short enough for many attempts in one test.
const TIMING: DownloadTiming = DownloadTiming {
    first_retry: Duration::from_millis(10),
    max_retry: Duration::from_millis(50),
    stall: Duration::from_millis(500),
};

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

fn client(loopback: NetworkAccess, private_networks: NetworkAccess) -> coral_outbound::Client {
    coral_outbound::Client::new(
        DestinationPolicy::new(loopback, private_networks),
        // Longer than every test waits, so only the stall bound ends a
        // silent server.
        Duration::from_mins(1),
    )
    .expect("client")
}

/// The delivery of the test manifest from `server` into `dirs`, on a CPU
/// that meets the floor.
fn delivery(server: &MockServer, dirs: &Dirs) -> Delivery {
    Delivery {
        manifest: manifest(server),
        models_dirs: vec![dirs.system.clone(), dirs.state.clone()],
        download_dir: dirs.state.clone(),
        client: client(NetworkAccess::HttpAndHttps, NetworkAccess::Deny),
        timing: TIMING,
        cpu: Ok(()),
    }
}

fn start(server: &MockServer, dirs: &Dirs) -> SearchModel {
    SearchModel::start(delivery(server, dirs)).0
}

/// Serves `body` at the release's `file`.
async fn serve(server: &MockServer, file: &str, body: &[u8]) {
    Mock::given(method("GET"))
        .and(path(format!("{RELEASE_PATH}/{file}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .mount(server)
        .await;
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

async fn ready(model: &SearchModel) -> Option<PathBuf> {
    tokio::time::timeout(PATIENCE, model.ready())
        .await
        .expect("the model is ready in time")
}

/// Takes `model`'s states until the delivery ends, and returns the last.
async fn last_state(model: &mut SearchModel) -> Option<SearchModelState> {
    tokio::time::timeout(PATIENCE, async {
        let mut last = None;
        while let Some(state) = model.next_state().await {
            last = Some(state);
        }
        last
    })
    .await
    .expect("the delivery ends in time")
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// The names in `dir`, sorted.
fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir).map_or_default(|entries| {
        entries
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect()
    });
    names.sort();
    names
}

/// The requests `server` has answered for `request_path`.
async fn requests_for(server: &MockServer, request_path: &str) -> usize {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|request| request.url.path() == request_path)
        .count()
}

/// Waits until `server` has answered `count` requests for the release's
/// `file`.
async fn requested(server: &MockServer, file: &str, count: usize) {
    let release_path = format!("{RELEASE_PATH}/{file}");
    tokio::time::timeout(PATIENCE, async {
        while requests_for(server, &release_path).await < count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_elapsed| panic!("{count} requests for {file} in time"));
}

#[tokio::test]
async fn absent_files_are_downloaded_checked_and_reported_ready() {
    for case in ["served by the release", "redirected to the asset host"] {
        let server = MockServer::start().await;
        if case == "served by the release" {
            serve(&server, "model.onnx", MODEL).await;
            serve(&server, "tokenizer.json", TOKENIZER).await;
        } else {
            // GitHub's redirect is absolute; a relative one works too.
            let absolute = format!("{}/assets/1?signature=x", server.uri());
            redirect(&server, "model.onnx", &absolute).await;
            redirect(&server, "tokenizer.json", "/assets/2").await;
            serve_asset(&server, "/assets/1", MODEL).await;
            serve_asset(&server, "/assets/2", TOKENIZER).await;
        }
        let dirs = Dirs::new();

        let model = start(&server, &dirs);

        assert_eq!(ready(&model).await, Some(dirs.downloaded()), "{case}");
        assert_eq!(read(&dirs.downloaded().join("model.onnx")), MODEL, "{case}");
        assert_eq!(
            read(&dirs.downloaded().join("tokenizer.json")),
            TOKENIZER,
            "{case}"
        );
        assert_eq!(
            names(&dirs.downloaded()),
            ["model.onnx", "tokenizer.json"],
            "{case}: a partial file stays"
        );
    }
}

#[tokio::test]
async fn a_download_may_take_longer_than_the_client_timeout() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("{RELEASE_PATH}/model.onnx")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(MODEL)
                .set_delay(Duration::from_millis(300)),
        )
        .mount(&server)
        .await;
    serve(&server, "tokenizer.json", TOKENIZER).await;
    let dirs = Dirs::new();

    // The model's answer takes longer than this client's own timeout, but
    // less than the stall bound, which is the only bound on a download.
    let (model, _delivery) = SearchModel::start(Delivery {
        client: coral_outbound::Client::new(
            DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny),
            Duration::from_millis(100),
        )
        .expect("client"),
        ..delivery(&server, &dirs)
    });

    assert_eq!(ready(&model).await, Some(dirs.downloaded()));
}

#[tokio::test]
async fn below_the_cpu_floor_nothing_is_downloaded() {
    let server = MockServer::start().await;
    serve(&server, "model.onnx", MODEL).await;
    serve(&server, "tokenizer.json", TOKENIZER).await;
    let dirs = Dirs::new();
    let too_old = CpuTooOld {
        missing: vec!["avx2", "fma"],
    };

    let (model, _delivery) = SearchModel::start(Delivery {
        cpu: Err(too_old.clone()),
        ..delivery(&server, &dirs)
    });

    assert_eq!(
        ready(&model).await,
        None,
        "the delivery ends without the files"
    );
    assert_eq!(
        model.state(),
        SearchModelState::Stopped(SearchModelStop::CpuTooOld(too_old))
    );
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        0
    );
    assert!(!dirs.downloaded().exists(), "a model folder was made");
}

#[tokio::test]
async fn a_file_with_another_digest_never_gets_the_final_name_and_is_downloaded_again_at_the_next_start()
 {
    let server = MockServer::start().await;
    serve(&server, "model.onnx", b"model bytez").await;
    serve(&server, "tokenizer.json", TOKENIZER).await;
    let dirs = Dirs::new();

    let mut model = start(&server, &dirs);

    assert_eq!(
        last_state(&mut model).await,
        Some(SearchModelState::Stopped(SearchModelStop::WrongFile))
    );
    assert_eq!(
        names(&dirs.downloaded()),
        ["tokenizer.json"],
        "a file with another digest got a name"
    );
    assert_eq!(
        requests_for(&server, &format!("{RELEASE_PATH}/model.onnx")).await,
        1,
        "the same process downloaded the file again"
    );

    server.reset().await;
    serve(&server, "model.onnx", MODEL).await;
    let next_start = start(&server, &dirs);

    assert_eq!(ready(&next_start).await, Some(dirs.downloaded()));
    assert_eq!(read(&dirs.downloaded().join("model.onnx")), MODEL);
}

#[tokio::test]
async fn a_failed_attempt_reports_why_and_when_the_next_one_starts() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let dirs = Dirs::new();

    let (mut model, _delivery) = SearchModel::start(Delivery {
        timing: DownloadTiming {
            first_retry: Duration::from_hours(1),
            ..TIMING
        },
        ..delivery(&server, &dirs)
    });

    let (reason, retry_in) = tokio::time::timeout(PATIENCE, async {
        loop {
            match model.next_state().await {
                Some(SearchModelState::Retrying { reason, retry_in }) => break (reason, retry_in),
                Some(_) => {}
                None => panic!("the delivery ended"),
            }
        }
    })
    .await
    .expect("a failed attempt in time");

    assert_eq!(retry_in, Duration::from_hours(1));
    assert!(reason.contains("500 Internal Server Error"), "{reason}");
    assert!(
        !reason.contains(RELEASE_PATH),
        "the reason names the URL: {reason}"
    );
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

        let model = start(&server, &dirs);

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

        let model = start(&server, &dirs);

        let expected = if in_system {
            dirs.system.join(MODEL_ID)
        } else {
            dirs.downloaded()
        };
        assert_eq!(ready(&model).await, Some(expected), "{case}");
        assert_eq!(
            server.received_requests().await.unwrap_or_default().len(),
            0,
            "{case}"
        );
        assert!(!other_model.exists(), "{case}: the other model stays");
    }
}

/// A file that the operator put in place but that Coral cannot read stops
/// the delivery with its path: a download over it would hide the cause.
#[cfg(unix)]
#[tokio::test]
async fn a_file_on_disk_that_cannot_be_read_stops_the_delivery_with_its_path() {
    let server = MockServer::start().await;
    serve(&server, "model.onnx", MODEL).await;
    serve(&server, "tokenizer.json", TOKENIZER).await;
    let dirs = Dirs::new();
    place_files(&dirs.system);
    // A link to itself: no user can open it, root included.
    let model_file = dirs.system.join(MODEL_ID).join("model.onnx");
    std::fs::remove_file(&model_file).expect("remove the model");
    std::os::unix::fs::symlink(&model_file, &model_file).expect("link the model to itself");

    let model = start(&server, &dirs);

    assert_eq!(
        ready(&model).await,
        None,
        "the delivery ends without the files"
    );
    let SearchModelState::Stopped(SearchModelStop::Unreadable(reason)) = model.state() else {
        panic!(
            "expected a stop at the unreadable file: {:?}",
            model.state()
        );
    };
    assert!(
        reason.contains(&model_file.display().to_string()),
        "{reason}"
    );
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        0
    );
    assert!(!dirs.downloaded().exists(), "a model folder was made");
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

    let model = start(&server, &dirs);
    requested(&server, "tokenizer.json", 2).await;

    assert!(other_model.exists(), "removed before the model is ready");

    server.reset().await;
    serve(&server, "model.onnx", MODEL).await;
    serve(&server, "tokenizer.json", TOKENIZER).await;
    assert_eq!(ready(&model).await, Some(dirs.downloaded()));
    assert!(!other_model.exists(), "kept after the model is ready");
}

#[tokio::test]
async fn the_destination_policy_checks_every_request_of_a_download() {
    // (case, loopback access, release requests that reach the server)
    let cases = [
        ("the release host", NetworkAccess::Deny, 0),
        (
            "a redirect to a private address",
            NetworkAccess::HttpAndHttps,
            1,
        ),
    ];
    for (case, loopback, release_requests) in cases {
        let server = MockServer::start().await;
        redirect(
            &server,
            "tokenizer.json",
            "http://10.255.255.1/tokenizer.json",
        )
        .await;
        let dirs = Dirs::new();
        let client = client(loopback, NetworkAccess::Deny);

        let attempt = download(
            &client,
            &manifest(&server),
            &dirs.downloaded(),
            TIMING.stall,
        )
        .await;

        assert!(
            matches!(attempt, Err(DownloadError::DestinationRejected)),
            "{case}: {attempt:?}"
        );
        assert_eq!(
            requests_for(&server, &format!("{RELEASE_PATH}/tokenizer.json")).await,
            release_requests,
            "{case}"
        );
        assert_eq!(names(&dirs.downloaded()), Vec::<String>::new(), "{case}");
    }
}
