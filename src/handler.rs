use crate::{database, Config};
use crate::{Context, Response};
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::Mutex;
pub struct AppState {
    pub conn: Mutex<redis::aio::Connection>,
    pub config: Config,
    pub cost_map: Mutex<HashMap<String, i64>>,
}
#[derive(Serialize)]
pub struct PingResponse {
    version: String,
}
#[derive(Deserialize)]
pub struct LoadRequest {
    key: String,
}
#[derive(Serialize)]
pub struct LoadResponse {
    value: String,
}

#[derive(Deserialize)]
pub struct StoreRequest {
    key: String,
    value: String,
    expiry: i64,
}

#[derive(Deserialize)]
pub struct ExistsRequest {
    key: String,
}
#[derive(Serialize)]
pub struct ExistsResponse {
    value: bool,
}

#[derive(Deserialize)]
pub struct ListRequest {
    prefix: String,
    is_recursive: bool,
}
#[derive(Serialize)]
pub struct ListResponse {
    keys_list: Vec<String>,
}
#[derive(Deserialize)]
pub struct StatRequest {
    key: String,
}

#[derive(Deserialize)]
pub struct DeleteRequest {
    key: String,
}
#[derive(Deserialize)]
pub struct LockRequest {
    key: String,
}
#[derive(Serialize)]
pub struct LockResponse {
    lock_id: Vec<u8>,
}

#[derive(Deserialize)]
pub struct UnlockRequest {
    key: String,
    lock_id: Vec<u8>,
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
                .body(v.into())
                .unwrap_or(internal_server_error());
        }
        Err(_) => {
            return internal_server_error();
        }
    }
}

fn get_pcr(req: &http::Request<hyper::body::Body>) -> Result<String, Box<dyn Error>> {
    match req.headers().get("pcr").ok_or(Err("pcr not found".into())) {
        Ok(value) => {
            return Ok(String::from(value.to_str()?));
        }
        Err(e) => {
            return e;
        }
    }
}

async fn update_cost(pcr: String, cost: i64, cost_map: &Mutex<HashMap<String, i64>>) {
    let mut map = cost_map.lock().await;
    *map.entry(pcr.to_owned()).or_default() += cost;
}

pub async fn ping(_ctx: Context) -> Response {
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
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let mut conn = ctx.state.conn.lock().await;
    let load_result =
        match database::load(pcr.to_owned(), &body.key, &mut conn, &ctx.state.config).await {
            Ok(value) => value,
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
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let mut conn = ctx.state.conn.lock().await;
    let cost = match database::store(
        pcr.to_owned(),
        &body.key,
        body.expiry,
        &body.value,
        &mut conn,
        &ctx.state.config,
    )
    .await
    {
        Ok(value) => value,
        Err(_) => {
            return internal_server_error();
        }
    };
    update_cost(pcr, cost, &ctx.state.cost_map).await;
    return Response::default();
}

pub async fn exists(mut ctx: Context) -> Response {
    let body: ExistsRequest = match ctx.body_json().await {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let pcr = match get_pcr(&ctx.req) {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let mut conn = ctx.state.conn.lock().await;

    let exists_result =
        match database::exists(pcr.to_owned(), &body.key, &mut *conn, &ctx.state.config).await {
            Ok(value) => value,
            Err(_) => {
                return internal_server_error();
            }
        };
    update_cost(pcr, exists_result.1, &ctx.state.cost_map).await;
    let resp = ExistsResponse {
        value: exists_result.0,
    };
    return json_response(&resp);
}

pub async fn list(mut ctx: Context) -> Response {
    let body: ListRequest = match ctx.body_json().await {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let pcr = match get_pcr(&ctx.req) {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let mut conn = ctx.state.conn.lock().await;

    let list_result = match database::list(
        pcr.to_owned(),
        &body.prefix,
        body.is_recursive,
        &mut *conn,
        &ctx.state.config,
    )
    .await
    {
        Ok(value) => value,
        Err(_) => {
            return internal_server_error();
        }
    };
    update_cost(pcr, list_result.1, &ctx.state.cost_map).await;
    let resp = ListResponse {
        keys_list: list_result.0,
    };
    return json_response(&resp);
}

pub async fn stat(mut ctx: Context) -> Response {
    let body: StatRequest = match ctx.body_json().await {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let pcr = match get_pcr(&ctx.req) {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let mut conn = ctx.state.conn.lock().await;

    let stat_result =
        match database::stat(pcr.to_owned(), &body.key, &mut *conn, &ctx.state.config).await {
            Ok(value) => value,
            Err(_) => {
                return internal_server_error();
            }
        };
    update_cost(pcr, stat_result.1, &ctx.state.cost_map).await;
    return json_response(&stat_result.0);
}

pub async fn delete(mut ctx: Context) -> Response {
    let body: DeleteRequest = match ctx.body_json().await {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let pcr = match get_pcr(&ctx.req) {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let mut conn = ctx.state.conn.lock().await;

    let delete_result =
        match database::delete(pcr.to_owned(), &body.key, &mut *conn, &ctx.state.config).await {
            Ok(value) => value,
            Err(_) => {
                return internal_server_error();
            }
        };
    update_cost(pcr, delete_result, &ctx.state.cost_map).await;
    return Response::default();
}

pub async fn lock(mut ctx: Context) -> Response {
    let body: LockRequest = match ctx.body_json().await {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let pcr = match get_pcr(&ctx.req) {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let mut conn = ctx.state.conn.lock().await;

    let lock_result =
        match database::lock(pcr.to_owned(), &body.key, &mut *conn, &ctx.state.config).await {
            Ok(value) => value,
            Err(_) => {
                return internal_server_error();
            }
        };
    update_cost(pcr, lock_result.1, &ctx.state.cost_map).await;
    let resp = LockResponse {
        lock_id: lock_result.0,
    };
    return json_response(&resp);
}

pub async fn unlock(mut ctx: Context) -> Response {
    let body: UnlockRequest = match ctx.body_json().await {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let pcr = match get_pcr(&ctx.req) {
        Ok(v) => v,
        Err(e) => {
            return bad_request_response(e);
        }
    };
    let mut conn = ctx.state.conn.lock().await;

    let unlock_result = match database::unlock(
        pcr.to_owned(),
        &body.key,
        &body.lock_id,
        &mut *conn,
        &ctx.state.config,
    )
    .await
    {
        Ok(value) => value,
        Err(_) => {
            return internal_server_error();
        }
    };
    update_cost(pcr, unlock_result, &ctx.state.cost_map).await;
    return Response::default();
}
