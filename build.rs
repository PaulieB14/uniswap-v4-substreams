use anyhow::{Ok, Result};
use substreams_ethereum::Abigen;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=abi");
    println!("cargo:rerun-if-changed=proto");

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
