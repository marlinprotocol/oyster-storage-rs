use crate::{Context, Response, router::IntoResponse};
use std::{collections::HashMap};
use serde::{Serialize, Deserialize};
use hyper::StatusCode;
use std::{error::Error};
use crate::{database, Config};
use tokio::sync::Mutex;
pub struct AppState {
    pub conn: Mutex<redis::aio::Connection>,
    pub config: Config,
    pub cost_map: Mutex<HashMap<String, i64>>,
}

pub async fn test_handler(ctx: Context) -> String {
    format!("test called, state_thing was: ")
}

#[derive(Serialize)]
pub struct PingResponse {
    version: String
}
#[derive(Deserialize)]
pub struct LoadRequest {
    key: String
}
#[derive(Serialize)]
pub struct LoadResponse {
    value: String
}

#[derive(Deserialize)]
pub struct StoreRequest {
    key: String,
    value: String,
    expiry: i64
}

fn internal_server_error() -> Response {
    let mut resp = Response::default();
    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    return resp;
}

fn bad_request_error() -> Response {
    let mut resp = Response::default();
    *resp.status_mut() = StatusCode::BAD_REQUEST;
    return resp;
}

fn bad_request_response(e: Box<dyn Error>) -> Response {
    hyper::Response::builder()
    .status(StatusCode::BAD_REQUEST)
    .body(format!("could not parse JSON: {}", e).into())
    .unwrap_or(bad_request_error())
}

fn json_response<T>(val: &T) -> Response
where
    T: ?Sized + Serialize,
{
    match serde_json::to_string(val) {
        Ok(v) => {
            return hyper::Response::builder()
                .header("Content-Type", "application/json")
                .body(v.into()).unwrap_or(internal_server_error());
        },
        Err(e) => {
            return internal_server_error();
        }
    }
}

fn get_pcr(req: &http::Request<hyper::body::Body>) -> Result<String, Box<dyn Error>> {
    match req.headers().get("pcr").ok_or( Err("pcr not found".into())) {
      Ok(value) => {
        return Ok(String::from(value.to_str()?));
      },
      Err(e) => {
        return e;
      }
    }
}

async fn update_cost(pcr : String, cost: i64, cost_map: &Mutex<HashMap<String, i64> >) {
    let mut map = cost_map.lock().await;
    *map.entry(pcr.to_owned()).or_default() += cost;

		// match cost_map.lock() {
		// 	Ok(mut map) => {
		// 		*map.entry(pcr.to_owned()).or_default() += cost;
		// 		println!("cost updated: {}", *map.entry(pcr).or_default());
		// 		return Ok(());
		// 	},
		// 	Err(e) => {
		// 		return Err(e.to_string().into());
		// 	}
		// };
}

pub async fn ping(ctx: Context) -> Response {
  println!("pong");
  let resp = PingResponse {
    version: "0.0.1".into(),
  };
  return json_response(&resp);
}

pub async fn load(mut ctx: Context) -> Response {
    let body: LoadRequest = match ctx.body_json().await {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
		let pcr = match get_pcr(&ctx.req) {
        Ok(v) => {
            v
        },
        Err(e) => {
            return bad_request_response(e);
        }
    };
		let mut conn = ctx.state.conn.lock().await;
		let load_result = match database::load(pcr.to_owned(), &body.key, &mut conn, &ctx.state.config).await {
			Ok(value) => {
				value
			},
			Err(_) => {
				return internal_server_error();
			}
		};
		update_cost(pcr, load_result.1, &ctx.state.cost_map).await;
		let resp = LoadResponse {
			value: load_result.0,
		};
		return json_response(&resp);
}


pub async fn store(mut ctx: Context) -> Response {
    let body: StoreRequest = match ctx.body_json().await {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
		let pcr = match get_pcr(&ctx.req) {
			Ok(v) => {
					v
			},
			Err(e) => {
					return bad_request_response(e);
			}
	};
	let mut conn = ctx.state.conn.lock().await;
	let cost = match database::store(pcr.to_owned(), &body.key, body.expiry, &body.value, &mut conn, &ctx.state.config).await {
			Ok(value) => {
				value
			},
			Err(_) => {
				return internal_server_error();
			}
	};
	update_cost(pcr, cost, &ctx.state.cost_map).await;
	return Response::default();
}

#[derive(Deserialize)]
struct SendRequest {
    name: String,
    active: bool,
}

pub async fn send_handler(mut ctx: Context) -> Response {
    let body: SendRequest = match ctx.body_json().await {
        Ok(v) => v,
        Err(e) => {
            return hyper::Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(format!("could not parse JSON: {}", e).into())
                .unwrap()
        }
    };

    Response::new(
        format!(
            "send called with name: {} and active: {}",
            body.name, body.active
        )
        .into(),
    )
}

pub async fn param_handler(ctx: Context) -> String {
    let param = match ctx.params.find("some_param") {
        Some(v) => v,
        None => "empty",
    };
    format!("param called, param was: {}", param)
}