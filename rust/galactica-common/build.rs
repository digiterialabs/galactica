fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = &[
        "../../proto/galactica/common/v1/common.proto",
        "../../proto/galactica/control/v1/auth.proto",
        "../../proto/galactica/control/v1/control.proto",
        "../../proto/galactica/node/v1/node.proto",
        "../../proto/galactica/gateway/v1/gateway.proto",
        "../../proto/galactica/artifact/v1/artifact.proto",
        "../../proto/galactica/runtime/v1/runtime.proto",
    ];

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos_with_config(config, protos, &["../../proto"])?;

    Ok(())
}
