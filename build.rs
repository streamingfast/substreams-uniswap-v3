use anyhow::{Ok, Result};
use substreams_ethereum::Abigen;

fn main() -> Result<(), anyhow::Error> {
    Abigen::new("pool", "abis/pool.json")?
        .generate()?
        .write_to_file("src/abi/pool.rs")?;
    Abigen::new("erc20", "abis/ERC20.json")?
        .generate()?
        .write_to_file("src/abi/erc20.rs")?;
    Abigen::new("factory", "abis/factory.json")?
        .generate()?
        .write_to_file("src/abi/factory.rs")?;
    Abigen::new("positionmanager", "abis/NonfungiblePositionManager.json")?
        .generate()?
        .write_to_file("src/abi/positionmanager.rs")?;

    Ok(())
}
