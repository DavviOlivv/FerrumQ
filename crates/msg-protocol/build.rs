fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Build scripts run as a single process before crate compilation; setting
    // PROTOC here only affects prost/tonic code generation for this crate.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_prost_build::configure()
        .compile_protos(&["proto/ferrumq/dataplane/v1/dataplane.proto"], &["proto"])?;

    Ok(())
}
