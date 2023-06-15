use crate::Config;
use base64::{engine::general_purpose, Engine as _};
use hyper::{header, Body, Client, Request};
use hyper_tls::HttpsConnector;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io::Write;
use url::Url;
#[derive(Serialize, Deserialize, Debug)]
struct AddResponse {
    Name: String,
    Hash: String,
    Size: String,
}
pub async fn add(data: String, config: &Config) -> Result<String, Box<dyn Error>> {
    println!("adding to ipfs {}", data);
    let boundary = "----WebKitFormBoundaryP7QTR7KAEBq0gxMo";
    let mut bodydata = Vec::new();
    write!(bodydata, "--{}\r\n", boundary)?;
    write!(
        bodydata,
        "Content-Disposition: form-data; name=\"file\"; filename=\"blob\"\r\n"
    )?;
    write!(bodydata, "Content-Type: application/octet-stream\r\n")?;
    write!(bodydata, "\r\n")?;
    write!(bodydata, "{}", data)?;
    write!(bodydata, "\r\n")?;
    write!(bodydata, "--{}--\r\n", boundary)?;
    let url = Url::parse(&(config.ipfs_url.clone() + "add"))?;

    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);
    let request = Request::post(url.as_str())
        .header(
            "Content-Type",
            &*format!("multipart/form-data; boundary={}", boundary),
        )
        .header(
            header::AUTHORIZATION,
            format!(
                "Basic {}",
                general_purpose::STANDARD_NO_PAD
                    .encode(format!("{}:{}", config.ipfs_key, config.ipfs_secret))
            ),
        )
        .body(bodydata.into())?;
    let resp = client.request(request).await?;
    println!("response {:?}", resp);
    if resp.status() == http::StatusCode::OK {
        let bytes = hyper::body::to_bytes(resp.into_body()).await?;
        let value: AddResponse = serde_json::from_slice(&bytes)?;
        println!("addedto ipfs {}", value.Hash);
        return Ok(value.Hash);
    }
    return Err("NON 200 status".into());
}

pub async fn delete(key: String, config: &Config) -> Result<(), Box<dyn Error>> {
    let mut url = Url::parse(&(config.ipfs_url.clone() + "pin/rm"))?;
    println!("deleting from ipfs {}", key);
    url.query_pairs_mut().append_pair("arg", &key);

    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);
    let request = Request::post(url.as_str())
        .header(
            header::AUTHORIZATION,
            format!(
                "Basic {}",
                general_purpose::STANDARD_NO_PAD
                    .encode(format!("{}:{}", config.ipfs_key, config.ipfs_secret))
            ),
        )
        .body(Body::empty())?;
    let resp = client.request(request).await?;

    if resp.status() == http::StatusCode::OK {
        return Ok(());
    }
    return Err("NON 200 status".into());
}

pub async fn get(key: String, config: &Config) -> Result<String, Box<dyn Error>> {
    println!("getting from ipfs {}", key);
    let mut url = Url::parse(&(config.ipfs_url.clone() + "cat"))?;

    url.query_pairs_mut().append_pair("arg", &key);

    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);
    let request = Request::post(url.as_str())
        .header(
            header::AUTHORIZATION,
            format!(
                "Basic {}",
                general_purpose::STANDARD_NO_PAD
                    .encode(format!("{}:{}", config.ipfs_key, config.ipfs_secret))
            ),
        )
        .body(Body::empty())?;
    let resp = client.request(request).await?;
    println!("response {:?}", resp);
    if resp.status() == http::StatusCode::OK {
        let bytes = hyper::body::to_bytes(resp.into_body()).await?;
        return Ok(String::from_utf8(bytes.to_vec())?);
    }
    return Err("NON 200 status".into());
}
