// use actix_web::dev::AppService;
use actix_web::web::Data;
use std::collections::HashMap;
use std::{error::Error};
use tokio::net::TcpListener;
use serde_derive::{Serialize, Deserialize};

use std::sync::{Arc};
use tokio::sync::Mutex;

use hyper::{
    server::conn::Http,
    body::to_bytes,
    service::{ service_fn},
    Body, Request
};

use route_recognizer::Params;
use router::Router;

use oyster::MolluskStream;
mod database;
mod handler;
mod router;
type Response = hyper::Response<hyper::Body>;
//type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
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


pub struct Context {
    pub state: Data<handler::AppState>,
    pub req: Request<Body>,
    pub params: Params,
    //body_bytes: Option<Bytes>,
}

async fn hello(_req: Request<Body>) -> Result<http::Response<Body>, std::convert::Infallible> {
    println!("server: hello");
    Ok(Response::new(Body::from("Hello World!")))
}
#[actix_web::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let key = [0u8; 64];
    let config: Config = confy::load_path("./config.toml")?;
    let conn = database::connect().await?;
    let cost_map: HashMap<String, i64> = HashMap::new();
    let server = TcpListener::bind("127.0.0.1:2100").await?;
    let app_state = Data::new(handler::AppState{
        conn: Mutex::new(conn),
        config: config,
        cost_map: Mutex::new(cost_map),
    });
    let mut router: router::Router = router::Router::new();
    router.get("/ping", Box::new(handler::ping));
    router.get("/load", Box::new(handler::load));
    //router.get("/store", Box::new(handler::store));
    router.get("/test", Box::new(handler::test_handler));
    router.post("/send", Box::new(handler::send_handler));
    router.get("/params/:some_param", Box::new(handler::param_handler));

    // let addr = "127.0.0.1:2100".parse().expect("address creation works");
    // let server = hyper::Server::bind(&addr).serve(new_service);
    // println!("Listening on http://{}", addr);
    // let _ = server.await;
    let shared_router = Arc::new(router);
    loop {
        let (stream, _) = server.accept().await?;
        let router_capture = shared_router.clone();
        let ss: MolluskStream = MolluskStream::new_server(stream, key).await?;
        let app_state = app_state.clone();
        //println!("{:?}", ss);

        tokio::task::spawn(async move {
            if let Err(http_err) = Http::new()
                .http1_only(true)
                .http1_keep_alive(true)
                .serve_connection(ss, service_fn(move |req| {
                    route(router_capture.clone(), req, app_state.clone())
                }))
                .await
            {
                eprintln!("Error while serving HTTP connection: {}", http_err);
            }
        });
    }
    Ok(())
}

async fn route(
    router: Arc<Router>,
    req: Request<hyper::Body>,
    app_state: Data<handler::AppState>,
) -> Result<Response, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let found_handler = router.route(req.uri().path(), req.method());
    let resp = found_handler
        .handler
        .invoke(Context::new(app_state, req, found_handler.params))
        .await;
    Ok(resp)
}

impl Context {
    pub fn new(state: Data<handler::AppState>, req: Request<Body>, params: Params) -> Context {
        Context {
            state,
            req,
            params,
            //body_bytes: None,
        }
    }

    pub async fn body_json<T: serde::de::DeserializeOwned>(&mut self) -> Result<T, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let body = to_bytes(self.req.body_mut()).await?;
        Ok(serde_json::from_slice(&body)?)

        // let body_bytes = match self.body_bytes {
        //     Some(ref v) => v,
        //     _ => {
        //         let body = to_bytes(self.req.body_mut()).await?;
        //         self.body_bytes = Some(body);
        //         self.body_bytes.as_ref().expect("body_bytes was set above")
        //     }
        // };
        
    }
}