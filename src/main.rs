use actix_web::{ App, web, Result, HttpServer,};
use std::collections::HashMap;
use std::{error::Error};

pub mod database;
pub mod handler;
use std::sync::Mutex;
use serde_derive::{Serialize, Deserialize};
#[derive(Serialize, Deserialize)]
pub struct Config {
    retry_delay: u64,
    retry_count: u64,
    lock_expiry: u64,
    operation_a_cost: i64,
    operation_b_cost: i64,
    operation_c_cost: i64,
    memory_cost: i64,
}

/// `Config` implements `Default`
impl ::std::default::Default for Config {
    fn default() -> Self { Self { 
        retry_delay: 200, // in millisecond
        retry_count: 5,
        lock_expiry: 30000, // in millesecond
        operation_a_cost: 17637500, // (in 10^-15 $) list
        operation_b_cost: 3527500, // (in 10^-15 $) store, load, stat
        operation_c_cost: 1763750, // (in 10^-15 $) exists
        memory_cost: 879583 } } // cost per Byte per millisecond (in 10^-23 $)
}
#[actix_web::main]
async fn main() -> Result<(), Box<dyn Error>>{

    let config: Config = confy::load_path("./config.toml")?;
    let conn = database::connect().await?;
    let cost_map: HashMap<String, i64> = HashMap::new();

    let appstate = web::Data::new(handler::AppState{
        conn: Mutex::new(conn),
        config: config,
        cost_map: Mutex::new(cost_map),
    });

    HttpServer::new(move || {
        App::new()
        .app_data(appstate.clone())
        .service(handler::ping)
        .service(handler::load)
        .service(handler::store)
        .service(handler::exists)
        .service(handler::list)
        .service(handler::stat)
        .service(handler::delete)
        .service(handler::lock)
        .service(handler::unlock)

    })
    .bind(("127.0.0.1",8080))?.run().await?;
    Ok(())
}

