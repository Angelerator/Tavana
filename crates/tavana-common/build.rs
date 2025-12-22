use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../../proto");
    
    let protos = [
        proto_root.join("tavana/v1/common.proto"),
        proto_root.join("tavana/v1/query.proto"),
    ];

    // Re-run if proto files change
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    // Only compile if proto files exist
    if protos.iter().all(|p| p.exists()) {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&protos, &[proto_root])?;
    } else {
        println!("cargo:warning=Proto files not found, skipping protobuf generation");
    }

    Ok(())
}

