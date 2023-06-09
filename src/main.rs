use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use tokio::net::TcpListener;

use std::sync::Arc;
use tokio::sync::Mutex;

use hyper::{body::to_bytes, server::conn::Http, service::service_fn, Body, Request};

use route_recognizer::Params;
use router::Router;

use oyster::MolluskStream;
mod database;
mod handler;
mod ipfs;
mod router;
type Response = hyper::Response<hyper::Body>;

#[derive(Serialize, Deserialize)]
pub struct Config {
    retry_delay: u64,
    retry_count: u64,
    lock_expiry: u64,
    operation_a_cost: i64,
    operation_b_cost: i64,
    operation_c_cost: i64,
    memory_cost: i64,
    ipfs_url: String,
    mem_threshold: usize,
    ipfs_key: String,
    ipfs_secret: String,
}

/// `Config` implements `Default`
impl ::std::default::Default for Config {
    fn default() -> Self {
        Self {
            retry_delay: 200, // in millisecond
            retry_count: 5,
            lock_expiry: 30000,         // in millesecond
            operation_a_cost: 17637500, // (in 10^-15 $) list
            operation_b_cost: 3527500,  // (in 10^-15 $) store, load, stat
            operation_c_cost: 1763750,  // (in 10^-15 $) exists
            memory_cost: 879583,
            ipfs_url: "".to_string(),
            mem_threshold: 1000, // in bytes
            ipfs_key: "".to_string(),
            ipfs_secret: "".to_string(),
        }
    } // cost per Byte per millisecond (in 10^-23 $)
}

pub struct Context {
    pub state: Arc<handler::AppState>,
    pub req: Request<Body>,
    pub params: Params,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let key: [u8; 64] = std::fs::read(&args[1])?.try_into().unwrap();
    let config: Config = confy::load_path("./config.toml")?;
    let conn = database::connect().await?;
    let cost_map: HashMap<String, i64> = HashMap::new();
    let server = TcpListener::bind("127.0.0.1:8080").await?;
    let app_state = Arc::new(handler::AppState {
        conn: Mutex::new(conn),
        config: config,
        cost_map: Mutex::new(cost_map),
    });
    let mut router: router::Router = router::Router::new();
    router.get("/ping", Box::new(handler::ping));
    router.post("/load", Box::new(handler::load));
    router.post("/store", Box::new(handler::store));
    router.post("/exists", Box::new(handler::exists));
    router.post("/list", Box::new(handler::list));
    router.post("/stat", Box::new(handler::stat));
    router.post("/delete", Box::new(handler::delete));
    router.post("/lock", Box::new(handler::lock));
    router.post("/unlock", Box::new(handler::unlock));

    let shared_router = Arc::new(router);
    loop {
        let (stream, _) = server.accept().await?;
        let router_capture = shared_router.clone();
        let app_state = app_state.clone();

        tokio::task::spawn(async move {
            match MolluskStream::new_server(stream, key).await {
                Ok(ss) => {
                    if let Err(http_err) = Http::new()
                        .http1_only(true)
                        .http1_keep_alive(true)
                        .serve_connection(
                            ss,
                            service_fn(move |req| {
                                route(router_capture.clone(), req, app_state.clone())
                            }),
                        )
                        .await
                    {
                        eprintln!("Error while serving HTTP connection: {}", http_err);
                    }
                }
                Err(e) => {
                    eprintln!("Error while serving HTTP connection: {}", e);
                }
            }
        });
    }
}

async fn route(
    router: Arc<Router>,
    req: Request<hyper::Body>,
    app_state: Arc<handler::AppState>,
) -> Result<Response, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let found_handler = router.route(req.uri().path(), req.method());
    let resp = found_handler
        .handler
        .invoke(Context::new(app_state, req, found_handler.params))
        .await;
    Ok(resp)
}

impl Context {
    pub fn new(state: Arc<handler::AppState>, req: Request<Body>, params: Params) -> Context {
        Context { state, req, params }
    }
    pub async fn body_json<T: serde::de::DeserializeOwned>(
        &mut self,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let body = to_bytes(self.req.body_mut()).await?;
        Ok(serde_json::from_slice(&body)?)
    }
}
