use std::thread::sleep;
use std::io::{self, Read};
use std::fs::File;
use std::time::{Duration};
use std::{error::Error};
use chrono::{Utc};
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use std::path::{Path};
use std::cmp;
use redis::AsyncCommands;
//use rslock::LockManager;

#[derive(Serialize, Deserialize, Debug)]
pub struct KeyInfo {
  key: String,
  modified: i64,
  size: usize,
  is_terminal: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct StorageData {
  value: String,
  modified: i64,
}
const RETRY_DELAY: u64 = 200;
const RETRY_COUNT: u64 = 5;
const LOCK_EXPIRY: u64 = 30000;
const OPERATION_A_COST: i64 = 100; // TODO
const OPERATION_B_COST: i64 = 50; // TODO
const OPERATION_C_COST: i64 = 0;
const MEMORY_UNIT_COST: i64 = 31665; // cost per Byte (in 10^-15 $) per hour

pub async fn connect() -> Result<redis::aio::Connection, Box<dyn Error>> {
  let redis_host_name = "127.0.0.1/";
  //let redis_password = "";

  let redis_conn_url = format!("redis://{}", redis_host_name);
  let conn = redis::Client::open(redis_conn_url)?.get_async_connection().await?;
  
  Ok(conn)
}

pub async fn load(pcr: String, key: &String, conn: &mut redis::aio::Connection) -> Result<(String, i64), Box<dyn Error>>{
  let key = get_namespaced_key(&pcr, key);
  let value: String = redis::cmd("GET")
   .arg(key)
  .query_async(conn).await?;

  let value: StorageData = serde_json::from_str(&String::from(value))?;
  Ok((value.value, OPERATION_B_COST))
}

async fn load_locked(pcr: String, key: &String, conn: &mut redis::aio::Connection) -> Result<Vec<u8>, Box<dyn Error>> {
  let key = get_locked_key(&pcr, key);
  let value = redis::cmd("GET")
   .arg(key)
  .query_async(conn).await?;

  Ok(value)
}

pub async fn store(pcr: String, key: &String, exp: i64, value: &String, conn: &mut redis::aio::Connection) -> Result<i64, Box<dyn Error>>{
  let key = get_namespaced_key(&pcr, key);
  let data = StorageData{
    value: String::from(value),
    modified: Utc::now().timestamp_millis(),
  };
  let mut cost = value.len() as i64;
  let value = serde_json::to_string(&data)?;
  if exp > 0 {
    cost = (key.len() as i64 + cost) as i64 * exp;
    redis::cmd("SET").arg(key).arg(value)
    .arg("PX").arg(exp).query_async(conn).await?;
  } else if exp == -1 { // only set the key if it already exist.
    let old_value: String = redis::cmd("SET").arg(key).arg(value)
    .arg("XX").arg("GET").arg("KEEPTTL")
    .query_async(conn).await?;
    cost = cmp::max(cost - old_value.len() as i64, 0) * exp;
  } else {
    return Err("expiry cannot be zero".into());
  }
  Ok(cost * MEMORY_UNIT_COST + OPERATION_A_COST)
}

async fn store_locked(pcr: String, key: &String, value: &[u8], conn: &mut redis::aio::Connection) -> Result<bool, Box<dyn Error>>{
  let key = get_locked_key(&pcr, key);

  let res: bool = redis::cmd("SET")
  .arg(key)
  .arg(value)
  .arg("NX")
  .arg("PX")
  .arg(LOCK_EXPIRY)
  .query_async(conn).await?;
  Ok(res)
}

pub async fn delete(pcr: String, key: &String, conn: &mut redis::aio::Connection) -> Result<i64, Box<dyn Error>>{
  let key = get_namespaced_key(&pcr, key);
  redis::cmd("DEL")
  .arg(key)
  .query_async(conn).await?;
  Ok(OPERATION_C_COST)
}

pub async fn delete_locked(pcr: String, key: &String, conn: &mut redis::aio::Connection) -> Result<(), Box<dyn Error>>{
  let key = get_locked_key(&pcr, key);
  redis::cmd("DEL")
  .arg(key)
  .query_async(conn).await?;
  Ok(())
}

pub async fn exists(pcr: String, key: &String, conn: &mut redis::aio::Connection) -> Result<(bool, i64), Box<dyn Error>>{
  let key = get_namespaced_key(&pcr, key);
  let ans: bool = conn.exists(key).await?;
  Ok((ans, OPERATION_B_COST))
}

async fn exists_locked(pcr: String, key: &String, conn: &mut redis::aio::Connection) -> Result<bool, Box<dyn Error>> {
  let key = get_locked_key(&pcr, key);
  let ans: bool = conn.exists(key).await?;
  Ok(ans)
}

pub async fn list(pcr: String, prefix: &String, recursive: bool, conn: &mut redis::aio::Connection) -> Result<(Vec<String>, i64), Box<dyn Error>> {

  let mut keysfound: Vec<String>  = Vec::new();
  let firstpointer = 0;
  let mut pointer = 0;
  let search: String;

  if prefix == "*" || prefix.trim().len() == 0 {
      search = get_namespaced_key(&pcr, &String::from("*"));
  } else {
      search = get_namespaced_key(&pcr, &String::from(prefix)) + "*";
  }
  
  loop {
      let mut res :(i32, Vec<String>) = redis::cmd("SCAN")
      .arg(pointer)
      .arg("MATCH")
      .arg(&search)
      .arg("COUNT")
      .arg(1)
      .query_async(conn).await?;
      
      for prefixed_key in &mut res.1 {
        match prefixed_key.strip_prefix(&get_namespace_prefix(&pcr)) {
          Some(val) => keysfound.push(String::from(val)),
          _ => (),
        }
      }
      //keysfound.append(&mut res.1);
      pointer = res.0;
      if firstpointer == pointer {
        break;
      }
  }

  if recursive || prefix == "*" || prefix.trim().len() == 0 {
    return Ok((keysfound, OPERATION_A_COST));
  }

  let mut keysmap = HashSet::new();

  for key in &keysfound {
    let dir = key.strip_prefix(&(prefix.to_owned())).unwrap_or("").split('/').next();
    match dir {
        Some(val) => keysmap.insert(String::from(val)),
        None => false,
    };
  }
  keysfound.clear();
  for key in keysmap {
    let found = prefix.to_owned() + &key;//Path::new(prefix).join(key).into_os_string().into_string();
    keysfound.push(found);
    // match found {
    //   Ok(val) => keysfound.push(val),
    //   _ => (),
    // };
  }
  Ok((keysfound, OPERATION_A_COST))
}

pub async fn stat(pcr: String, key: &String, conn: &mut redis::aio::Connection) -> Result<(KeyInfo, i64), Box<dyn Error>> {
  let prefixed_key = get_namespaced_key(&pcr, key);
  let value: String = redis::cmd("GET")
   .arg(prefixed_key)
  .query_async(conn).await?;

  let value: StorageData = serde_json::from_str(&String::from(value))?;
  Ok((KeyInfo{
    key: String::from(key),
    modified: value.modified,
    size: value.value.len(),
    is_terminal: !key.ends_with('/'),
  }, OPERATION_B_COST))
}

fn get_namespaced_key(pcr: &String, key: &String) -> String {
  get_namespace_prefix(&pcr) + key
}

fn get_namespace_prefix(pcr: &String) -> String {
  String::from(pcr) + "/"
}

fn get_locked_key(pcr: &String, key: &String) -> String {
  get_locked_prefix(&pcr) + key
}

fn get_locked_prefix(pcr: &String) -> String {
  String::from(pcr) + ".lock" + "/"
}

pub fn get_unique_lock_id() -> io::Result<Vec<u8>> {
  let file = File::open("/dev/urandom")?;
  let mut buf = Vec::with_capacity(20);
  match file.take(20).read_to_end(&mut buf) {
      Ok(20) => Ok(buf),
      Ok(_containers) => Err(io::Error::new(
          io::ErrorKind::Other,
          "Can't read enough random bytes",
      )),
      Err(e) => Err(e),
  }
}

pub async fn lock(pcr: String, key: &String, conn: &mut redis::aio::Connection) -> Result<(Vec<u8>, i64), Box<dyn Error>>{
  
  for _ in 0..RETRY_COUNT {
    if exists_locked(pcr.clone(), key, conn).await? {
      sleep(Duration::from_millis(RETRY_DELAY)); // TODO: change to async
    } else {
      let val = get_unique_lock_id()?;
      if store_locked(pcr, key, &val, conn).await? {
        return Ok((val, OPERATION_A_COST));
      } else {
        break; 
      }
    }
  }
  Err("Can't obtain lock".into())
}

pub async fn unlock(pcr: String, key: &String, lock_id: &[u8], conn: &mut redis::aio::Connection) -> Result<i64, Box<dyn Error>>{
  if load_locked(pcr.clone(), key, conn).await?.eq(lock_id) {
    match delete_locked(pcr, key, conn).await {
      Ok(()) => {
        return Ok(OPERATION_C_COST);
      },
      Err(err) => {
        return Err(err);
      }
    }
  } else {
    return Err("lock_id mismatch".into());
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test] 
  async fn test_connection() -> Result<(), Box<dyn Error>> {
    connect().await?;
    Ok(())
  }

  #[tokio::test]
  async fn test_store() -> Result<(), Box<dyn Error>> {
    let mut conn = connect().await?;
    store(String::from("pcr"), &String::from("test_store"), 1000, &String::from("This is a test value"), &mut conn).await?;
    Ok(())
  }

  #[tokio::test]
  async fn test_load() -> Result<(), Box<dyn Error>> {
    let mut conn = connect().await?;
    store(String::from("pcr"), &String::from("test_load"), 1000, &String::from("This is a test value"), &mut conn).await?;
    let val = load(String::from("pcr"), &String::from("test_load"), &mut conn).await?;
    assert_eq!(val.0, String::from("This is a test value"));
    Ok(())
  }

  #[tokio::test]
  async fn test_store_expiry() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    store(String::from("pcr"), &String::from("test_store_expiry"), 1000, &String::from("This is a test value"), &mut conn).await?;
    sleep(Duration::from_millis(1000));
    load(String::from("pcr"), &String::from("test_store_expiry"), &mut conn).await.expect_err("should not load");
    Ok(())
  }
  
  #[tokio::test]
  async fn test_store_keepttl() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    store(String::from("pcr"), &String::from("test_store_keepttl"), 1000, &String::from("This is a test value"), &mut conn).await?;
    sleep(Duration::from_millis(400));
    store(String::from("pcr"), &String::from("test_store_keepttl"), -1, &String::from("This is a test value"), &mut conn).await?;
    sleep(Duration::from_millis(400));
    load(String::from("pcr"), &String::from("test_store_keepttl"), &mut conn).await?;
    sleep(Duration::from_millis(400));
    load(String::from("pcr"), &String::from("test_store_keepttl"), &mut conn).await.expect_err("should not load");
    Ok(())
  }

  #[tokio::test]
  async fn test_store_zeroexpiry() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    store(String::from("pcr"), &String::from("test_store_zeroexpiry"), 0, &String::from("This is a test value"), &mut conn).await.expect_err("should not store zero expiry");
    Ok(())
  }
  #[tokio::test]
  async fn test_exists() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    store(String::from("pcr"), &String::from("test_exists"), 1000, &String::from("This is a test value"), &mut conn).await?;
    let check = exists(String::from("pcr"), &String::from("test_exists"), &mut conn).await?;
    assert_eq!(true, check.0);
    let check = exists(String::from("pcr"), &String::from("not_in_db"), &mut conn).await?;
    assert_eq!(false, check.0);
    Ok(())
  }

  #[tokio::test]
  async fn test_delete() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    store(String::from("pcr"), &String::from("test_delete"), 1000, &String::from("This is a test value"), &mut conn).await?;
    delete(String::from("pcr"), &String::from("test_delete"), &mut conn).await?;
    let check = exists(String::from("pcr"), &String::from("test_delete"), &mut conn).await?;
    assert_eq!(false, check.0);
    Ok(())
  }

  #[tokio::test]
  async fn test_stat() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    store(String::from("pcr"), &String::from("test_stat"), 1000, &String::from("This is a test value"), &mut conn).await?;
    let info = stat(String::from("pcr"), &String::from("test_stat"), &mut conn).await?;
    assert_eq!("test_stat", info.0.key);
    assert_eq!("This is a test value".len(), info.0.size);
    assert_eq!(true, info.0.is_terminal);
    Ok(())
  }

  #[tokio::test]
  async fn test_lock() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    
    lock(String::from("pcr"), &String::from("test_lock"), &mut conn).await?;
    lock(String::from("pcr"), &String::from("test_lock"), &mut conn).await.expect_err("lock not obtained");
    Ok(())
  }

  #[tokio::test]
  async fn test_lock_expiry() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    
    lock(String::from("pcr"), &String::from("test_lock_expiry"), &mut conn).await?;
    sleep(Duration::from_millis(LOCK_EXPIRY));
    lock(String::from("pcr"), &String::from("test_lock_expiry"), &mut conn).await?;
    Ok(())
  }

  #[tokio::test]
  async fn test_unlock() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    
    let lock_id = lock(String::from("pcr"), &String::from("test_unlock"), &mut conn).await?;
    unlock(String::from("pcr"), &String::from("test_unlock"), &lock_id.0, &mut conn).await?;
    lock(String::from("pcr"), &String::from("test_unlock"), &mut conn).await?;
    Ok(())
  }

  #[tokio::test]
  async fn test_list_recursive() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    store(String::from("pcr"), &String::from("test_list_recursive_0"), 1000, &String::from("This is a test value"), &mut conn).await?;
    store(String::from("pcr"), &String::from("test_list_recursive/1"), 1000, &String::from("This is a test value"), &mut conn).await?;
    store(String::from("pcr"), &String::from("test_list_recursive/2"), 1000, &String::from("This is a test value"), &mut conn).await?;
    store(String::from("pcr"), &String::from("unused_test_list_recursive"), 1000, &String::from("This is a test value"), &mut conn).await?;
    let list_result = list(String::from("pcr"), &String::from("test_list_recursive"), true, &mut conn).await?;
    assert_eq!(3, list_result.0.len());
    for i in &list_result.0 {
      print!("{}", i);
      if i.ne("test_list_recursive_0") && i.ne("test_list_recursive/1") && i.ne("test_list_recursive/2") {
        return Err("different key".into());
      }
    }
    Ok(())
  }

  #[tokio::test]
  async fn test_store_benchmark() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    
    use std::time::Instant;
    let now = Instant::now();
    {
      let mut i = 0;
      while i < 1000000  {
        store(String::from("test_store_benchmark_namespace"), &String::from("test_store_benchmark_key"), 1000, &String::from("This is a test value"), &mut conn).await?;
        i = i+1;
      }
    }
    let elapsed = now.elapsed();
    println!("test_store_benchmark 1000 calls Elapsed: {:.2?}", elapsed);
    Ok(())
  }

  #[tokio::test]
  async fn test_load_benchmark() -> Result<(), Box<dyn Error>>{
    let mut conn = connect().await?;
    let mut i = 0;
    while i < 10000  {
      store(String::from("test_load_benchmark_namespace"), &(String::from("test_load_benchmark_key") + &i.to_string()), 100000, &String::from("This is a test value"), &mut conn).await?;
      i = i+1;
    }

    use std::time::Instant;
    let now = Instant::now();
    {
      let mut i = 0;
      while i < 100000  {
        let _val = load(String::from("test_load_benchmark_namespace"), &String::from("test_load_benchmark_key"), &mut conn).await?;
        i = i+1;
      }
    }
    let elapsed = now.elapsed();
    println!("test_store_benchmark 1000 calls Elapsed: {:.2?}", elapsed);
    Ok(())
  }
}