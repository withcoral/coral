//! Reproducible private wire bindings; ordinary builds need no protobuf compiler.
use prost::Message as _;
use std::{fs, path::Path, process::Command};

use anyhow::{Context as _, Result, bail};

pub(super) fn generate(check: bool) -> Result<bool> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../crates/coral-grpc");
    let generated = root.join("src/generated");
    let temp = tempfile::tempdir()?;
    let descriptor = temp.path().join("private.binpb");
    let status = Command::new("buf")
        .current_dir(&root)
        .args(["build", "--as-file-descriptor-set", "--output"])
        .arg(&descriptor)
        .status()
        .context("run Buf to compile private protobufs")?;
    if !status.success() {
        bail!("Buf could not compile private protobufs");
    }
    let descriptors = prost_types::FileDescriptorSet::decode(fs::read(descriptor)?.as_slice())?;
    tonic_prost_build::configure()
        .out_dir(temp.path())
        .server_mod_attribute(".", "#[allow(dead_code, unreachable_pub)]")
        .compile_fds(descriptors)?;
    fs::create_dir_all(&generated)?;
    for name in ["coral.internal.v1.rs", "coral.management.v1.rs"] {
        // Tonic exposes no server-module visibility option. Keep its wrappers
        // crate-private: callers compose routes, not individual service servers.
        // Wire messages and generated clients retain their normal visibility.
        let content = fs::read_to_string(temp.path().join(name))?
            .lines()
            .map(|line| {
                if line.starts_with("pub mod ") && line.ends_with("_server {") {
                    line.replacen("pub mod ", "pub(crate) mod ", 1)
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let content = content.as_bytes();
        let target = generated.join(name);
        if check {
            let existing =
                fs::read(&target).with_context(|| format!("missing {}", target.display()))?;
            if existing != content {
                bail!("private bindings drifted; run cargo run -p xtask -- generate-proto");
            }
        } else {
            fs::write(target, content)?;
        }
    }
    Ok(true)
}
