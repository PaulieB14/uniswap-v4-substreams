use anyhow::{Ok, Result};
use substreams_ethereum::Abigen;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=abi");
    println!("cargo:rerun-if-changed=proto");

    // The .proto is compiled here rather than checked in via `substreams
    // protogen`, because protogen resolves the manifest's `imports:` over the
    // network (two spkgs on GitHub) and would make an offline `cargo build`
    // fail. prost-build only needs the local file plus protoc. Output lands in
    // $OUT_DIR/uniswap.v4.v1.rs, which src/pb.rs include!()s.
    prost_build::Config::new()
        .compile_protos(&["proto/uniswap/v4/v1/uniswap.proto"], &["proto"])?;

    Abigen::new("PoolManager", "abi/PoolManager.json")?
        .generate()?
        .write_to_file("src/abi/pool_manager.rs")?;
    Abigen::new("PositionManager", "abi/PositionManager.json")?
        .generate()?
        .write_to_file("src/abi/position_manager.rs")?;
    Abigen::new("ArrakisHookFactory", "abi/ArrakisHookFactory.json")?
        .generate()?
        .write_to_file("src/abi/arrakis.rs")?;
    Ok(())
}
