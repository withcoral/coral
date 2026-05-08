//! Build script for the generated `coral-api` `protobuf` and `tonic` bindings.

fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc);
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(
            config,
            &[
                "proto/coral/v1/catalog.proto",
                "proto/coral/v1/resources.proto",
                "proto/coral/v1/feedback.proto",
                "proto/coral/v1/sources.proto",
                "proto/coral/v1/query.proto",
            ],
            &["proto"],
        )
        .expect("compile coral v1 protobuf");
}
