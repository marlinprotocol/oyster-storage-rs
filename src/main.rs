use std::time::{Duration};
use handler::AppState;
use redis::Commands;
use actix_web::{error, App, get, web, Responder, Result, HttpRequest, HttpResponse, http::{header::ContentType, StatusCode}, HttpServer,};
use std::{error::Error, thread::sleep};
use futures::{executor::block_on};
use tokio;
pub mod database;
pub mod handler;
use std::sync::Mutex;
#[actix_web::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let conn = database::connect().await?;
    let appstate = web::Data::new(handler::AppState{
        conn: Mutex::new(conn),
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

