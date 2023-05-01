use actix_web::{error, get, post,  web, Responder, Result, HttpRequest, HttpResponse, http::{header::ContentType, StatusCode},};
use serde::{Serialize, Deserialize};
use std::{sync::Mutex, collections::HashMap};
use crate::{database, Config};

use derive_more::{Display, Error};

pub struct AppState {
  pub conn: Mutex<redis::aio::Connection>,
  pub config: Config,
  pub cost_map: Mutex::<HashMap<String, i64>>,
}

#[derive(Debug, Display, Error)]
pub enum UserError {
    #[display(fmt = "Validation error on field: {}", field)]
    ValidationError { field: String },
    InternalServerError,
}

impl error::ResponseError for UserError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::html())
            .body(self.to_string())
    }
    fn status_code(&self) -> StatusCode {
        match *self {
            UserError::ValidationError { .. } => StatusCode::BAD_REQUEST,
            UserError::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
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
#[derive(Serialize)]
pub struct PingResponse {
    version: String
}

#[derive(Deserialize)]
pub struct ExistsRequest {
    key: String
}
#[derive(Serialize)]
pub struct ExistsResponse {
    value: bool
}

fn get_pcr(req: &HttpRequest) -> Result<String, UserError> {
  match req.headers().get("pcr").ok_or(UserError::ValidationError { field: "pcr".into() })?.to_str() {
    Ok(value) => {
      return Ok(value.into());
    },
    Err(e) => {
      return Err(UserError::ValidationError { field: e.to_string() });
    }
  }
}

fn update_cost(pcr : String, cost: i64, cost_map: &Mutex::<HashMap<String, i64>>) -> Result<(), UserError> {
  match cost_map.lock() {
    Ok(mut map) => {
      *map.entry(pcr).or_default() += cost;
      //println!("cost updated: {}", *map.entry(pcr).or_default());
      return Ok(());
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };
}

#[post("/load")]
pub async fn load(state: web::Data<AppState>, body: web::Json<LoadRequest>, req: HttpRequest) -> Result<impl Responder, UserError> {
  let pcr: String = get_pcr(&req)?;
  let mut conn;
  match state.conn.lock() {
    Ok(connection) => {
      conn = connection;
    },
    Err(e) => {
      println!("error2 {}", e);
      return Err(UserError::InternalServerError);
    }
  };

  match database::load(pcr.to_owned(), &body.key, &mut conn, &state.config).await {
    Ok(value) => {
      update_cost(pcr, value.1, &state.cost_map).unwrap_or_default();
      let resp = LoadResponse {
        value: value.0,
      };
      return Ok(web::Json(resp));
    },
    Err(e) => {
      println!("error3 {}", e);
      return Err(UserError::InternalServerError);
    }
  };
}

#[get("/ping")]
pub async fn ping() -> Result<impl Responder, UserError> {
  println!("pong");
  let resp = PingResponse {
    version: "0.0.1".into(),
  };
  Ok(web::Json(resp))
}

#[post("/store")]
pub async fn store(state: web::Data<AppState>, body: web::Json<StoreRequest>, req: HttpRequest) -> Result<impl Responder, UserError> {
  let pcr = get_pcr(&req)?;
  let mut conn;
  match state.conn.lock() {
    Ok(connection) => {
      conn = connection;
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };

  match database::store(pcr.to_owned(), &body.key, body.expiry, &body.value, &mut *conn, &state.config).await {
    Ok(value) => {
      update_cost(pcr, value, &state.cost_map).unwrap_or_default();
      return Ok(HttpResponse::Ok().finish());
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };
}

#[post("/exists")]
pub async fn exists(state: web::Data<AppState>, body: web::Json<ExistsRequest>, req: HttpRequest) -> Result<impl Responder, UserError> {
  let pcr = get_pcr(&req)?;
  let mut conn;
  match state.conn.lock() {
    Ok(connection) => {
      conn = connection;
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };

  match database::exists(pcr.to_owned(), &body.key, &mut *conn, &state.config).await {
    Ok(value) => {
      update_cost(pcr, value.1, &state.cost_map).unwrap_or_default();
      let resp = ExistsResponse {
        value: value.0,
      };
      return Ok(web::Json(resp));
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };
}

#[derive(Deserialize)]
pub struct ListRequest {
    prefix: String,
    is_recursive: bool
}
#[derive(Serialize)]
pub struct ListResponse {
    keys_list: Vec<String>
}

#[post("/list")]
pub async fn list(state: web::Data<AppState>, body: web::Json<ListRequest>, req: HttpRequest) -> Result<impl Responder, UserError> {
  let pcr = get_pcr(&req)?;
  let mut conn;
  match state.conn.lock() {
    Ok(connection) => {
      conn = connection;
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };

  match database::list(pcr.to_owned(), &body.prefix, body.is_recursive, &mut *conn, &state.config).await {
    Ok(value) => {
      update_cost(pcr, value.1, &state.cost_map).unwrap_or_default();
      let resp = ListResponse {
        keys_list: value.0,
      };
      return Ok(web::Json(resp));
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };
}

#[derive(Deserialize)]
pub struct StatRequest {
    key: String
}

#[post("/stat")]
pub async fn stat(state: web::Data<AppState>, body: web::Json<StatRequest>, req: HttpRequest) -> Result<impl Responder, UserError> {
  let pcr = get_pcr(&req)?;
  let mut conn;
  match state.conn.lock() {
    Ok(connection) => {
      conn = connection;
      },
      Err(_) => {
        return Err(UserError::InternalServerError);
      }
  };

  match database::stat(pcr.to_owned(), &body.key, &mut *conn, &state.config).await {
    Ok(value) => {
      update_cost(pcr, value.1, &state.cost_map).unwrap_or_default();
      return Ok(web::Json(value.0));
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };
}



#[derive(Deserialize)]
pub struct DeleteRequest {
  key: String
}

#[post("/delete")]
pub async fn delete(state: web::Data<AppState>, body: web::Json<DeleteRequest>, req: HttpRequest) -> Result<impl Responder, UserError> {
  let pcr = get_pcr(&req)?;
  let mut conn;
  match state.conn.lock() {
    Ok(connection) => {
      conn = connection;
      },
      Err(_) => {
        return Err(UserError::InternalServerError);
      }
  };

  match database::delete(pcr.to_owned(), &body.key, &mut *conn, &state.config).await {
    Ok(value) => {
      update_cost(pcr, value, &state.cost_map).unwrap_or_default();
      return Ok(HttpResponse::Ok().finish());
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };
}

#[derive(Deserialize)]
pub struct LockRequest {
    key: String
}
#[derive(Serialize)]
pub struct LockResponse {
    lock_id: Vec<u8>
}
#[post("/lock")]
pub async fn lock(state: web::Data<AppState>, body: web::Json<LockRequest>, req: HttpRequest) -> Result<impl Responder, UserError> {
  let pcr = get_pcr(&req)?;
  let mut conn;
  match state.conn.lock() {
    Ok(connection) => {
      conn = connection;
      },
      Err(_) => {
        return Err(UserError::InternalServerError);
      }
  };

  match database::lock(pcr.to_owned(), &body.key, &mut *conn, &state.config).await {
    Ok(value) => {
      update_cost(pcr, value.1, &state.cost_map).unwrap_or_default();
      let resp = LockResponse {
        lock_id: value.0,
      };
      return Ok(web::Json(resp));
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };
}

#[derive(Deserialize)]
pub struct UnlockRequest {
    key: String,
    lock_id: Vec<u8>
}

#[post("/unlock")]
pub async fn unlock(state: web::Data<AppState>, body: web::Json<UnlockRequest>, req: HttpRequest) -> Result<impl Responder, UserError> {
  let pcr = get_pcr(&req)?;
  let mut conn;
  match state.conn.lock() {
    Ok(connection) => {
      conn = connection;
      },
      Err(_) => {
        return Err(UserError::InternalServerError);
      }
  };

  match database::unlock(pcr.to_owned(), &body.key, &body.lock_id, &mut *conn, &state.config).await {
    Ok(value) => {
      update_cost(pcr, value, &state.cost_map).unwrap_or_default();
      return Ok(HttpResponse::Ok().finish());
    },
    Err(_) => {
      return Err(UserError::InternalServerError);
    }
  };
}