use std::time::{Duration};
use redis::Commands;
use std::{error::Error, thread::sleep};
use futures::executor::block_on;
use tokio;
pub mod database;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let mut conn = database::connect().await?;
    Ok(())
}

