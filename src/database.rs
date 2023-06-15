use chrono::Utc;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::{self, Read};
use std::thread::sleep;
use std::time::Duration;

use crate::{ipfs, Config};
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
    ipfs: bool,
}

pub async fn connect() -> Result<redis::aio::Connection, Box<dyn Error>> {
    let redis_host_name = "127.0.0.1/";
    //let redis_password = "";

    let redis_conn_url = format!("redis://{}", redis_host_name);
    let conn = redis::Client::open(redis_conn_url)?
        .get_async_connection()
        .await?;

    Ok(conn)
}

pub async fn load(
    pcr: String,
    key: &String,
    conn: &mut redis::aio::Connection,
    config: &Config,
) -> Result<(String, i64), Box<dyn Error>> {
    let key = get_namespaced_key(&pcr, key);
    let value: String = redis::cmd("GET").arg(key).query_async(conn).await?;

    let mut value: StorageData = serde_json::from_str(&String::from(value))?;
    if value.ipfs {
        value.value = ipfs::get(value.value, config).await?;
    }
    Ok((value.value, config.operation_c_cost))
}

async fn load_locked(
    pcr: String,
    key: &String,
    conn: &mut redis::aio::Connection,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let key = get_locked_key(&pcr, key);
    let value = redis::cmd("GET").arg(key).query_async(conn).await?;

    Ok(value)
}

pub async fn store(
    pcr: String,
    key: &String,
    exp: i64,
    value: &String,
    conn: &mut redis::aio::Connection,
    config: &Config,
) -> Result<i64, Box<dyn Error>> {
    let key = get_namespaced_key(&pcr, key);
    let mut data = StorageData {
        ipfs: false,
        value: String::from(value),
        modified: Utc::now().timestamp_millis(),
    };
    if value.len() > config.mem_threshold {
        data.value = ipfs::add(value.to_string(), config).await?;
        data.ipfs = true;
    }
    let value = serde_json::to_string(&data)?;
    let mut cost = value.len() as i64;
    if exp > 0 {
        cost = key.len() as i64 + cost;
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("PX")
            .arg(exp)
            .query_async(conn)
            .await?;
    } else if exp == -1 {
        // only set the key if it already exist.
        let old_value: String = redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("XX")
            .arg("GET")
            .arg("KEEPTTL")
            .query_async(conn)
            .await?;
        cost = cmp::max(cost - old_value.len() as i64, 0);
    } else {
        return Err("expiry cannot be zero".into());
    }
    Ok(cost * (exp / 1000) * config.memory_cost + config.operation_c_cost)
}

async fn store_locked(
    pcr: String,
    key: &String,
    value: &[u8],
    conn: &mut redis::aio::Connection,
    config: &Config,
) -> Result<bool, Box<dyn Error>> {
    let key = get_locked_key(&pcr, key);

    let res: bool = redis::cmd("SET")
        .arg(key)
        .arg(value)
        .arg("NX")
        .arg("PX")
        .arg(config.lock_expiry)
        .query_async(conn)
        .await?;
    Ok(res)
}

pub async fn delete(
    pcr: String,
    key: &String,
    conn: &mut redis::aio::Connection,
    config: &Config,
) -> Result<i64, Box<dyn Error>> {
    let key = get_namespaced_key(&pcr, key);
    let value: String = redis::cmd("GET")
        .arg(key.to_string())
        .query_async(conn)
        .await?;
    if value.len() > 0 {
        let value: StorageData = serde_json::from_str(&String::from(value))?;
        if value.ipfs {
            ipfs::delete(value.value, config).await?;
        }
    }
    redis::cmd("DEL").arg(key).query_async(conn).await?;
    Ok(config.operation_c_cost)
}

pub async fn delete_locked(
    pcr: String,
    key: &String,
    conn: &mut redis::aio::Connection,
) -> Result<(), Box<dyn Error>> {
    let key = get_locked_key(&pcr, key);
    redis::cmd("DEL").arg(key).query_async(conn).await?;
    Ok(())
}

pub async fn exists(
    pcr: String,
    key: &String,
    conn: &mut redis::aio::Connection,
    config: &Config,
) -> Result<(bool, i64), Box<dyn Error>> {
    let key = get_namespaced_key(&pcr, key);
    let ans: bool = conn.exists(key).await?;
    Ok((ans, config.operation_c_cost))
}

async fn exists_locked(
    pcr: String,
    key: &String,
    conn: &mut redis::aio::Connection,
) -> Result<bool, Box<dyn Error>> {
    let key = get_locked_key(&pcr, key);
    let ans: bool = conn.exists(key).await?;
    Ok(ans)
}

pub async fn list(
    pcr: String,
    prefix: &String,
    recursive: bool,
    conn: &mut redis::aio::Connection,
    config: &Config,
) -> Result<(Vec<String>, i64), Box<dyn Error>> {
    let mut keysfound: Vec<String> = Vec::new();
    let firstpointer = 0;
    let mut pointer = 0;
    let search: String;

    if prefix == "*" || prefix.trim().len() == 0 {
        search = get_namespaced_key(&pcr, &String::from("*"));
    } else {
        search = get_namespaced_key(&pcr, &String::from(prefix)) + "*";
    }

    loop {
        let mut res: (i32, Vec<String>) = redis::cmd("SCAN")
            .arg(pointer)
            .arg("MATCH")
            .arg(&search)
            .arg("COUNT")
            .arg(1)
            .query_async(conn)
            .await?;

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
        return Ok((keysfound, config.operation_a_cost));
    }

    let mut keysmap = HashSet::new();

    for key in &keysfound {
        let dir = key
            .strip_prefix(&(prefix.to_owned()))
            .unwrap_or("")
            .split('/')
            .next();
        match dir {
            Some(val) => keysmap.insert(String::from(val)),
            None => false,
        };
    }
    keysfound.clear();
    for key in keysmap {
        let found = prefix.to_owned() + &key; //Path::new(prefix).join(key).into_os_string().into_string();
        keysfound.push(found);
        // match found {
        //   Ok(val) => keysfound.push(val),
        //   _ => (),
        // };
    }
    Ok((keysfound, config.operation_a_cost))
}

pub async fn stat(
    pcr: String,
    key: &String,
    conn: &mut redis::aio::Connection,
    config: &Config,
) -> Result<(KeyInfo, i64), Box<dyn Error>> {
    let prefixed_key = get_namespaced_key(&pcr, key);
    let value: String = redis::cmd("GET")
        .arg(prefixed_key)
        .query_async(conn)
        .await?;

    let value: StorageData = serde_json::from_str(&String::from(value))?;
    Ok((
        KeyInfo {
            key: String::from(key),
            modified: value.modified,
            size: value.value.len(),
            is_terminal: !key.ends_with('/'),
        },
        config.operation_c_cost,
    ))
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

pub async fn lock(
    pcr: String,
    key: &String,
    conn: &mut redis::aio::Connection,
    config: &Config,
) -> Result<(Vec<u8>, i64), Box<dyn Error>> {
    for _ in 0..config.retry_count {
        if exists_locked(pcr.clone(), key, conn).await? {
            sleep(Duration::from_millis(config.retry_delay)); // TODO: change to async
        } else {
            let val = get_unique_lock_id()?;
            if store_locked(pcr, key, &val, conn, config).await? {
                return Ok((val, config.operation_b_cost));
            } else {
                break;
            }
        }
    }
    Err("Can't obtain lock".into())
}

pub async fn unlock(
    pcr: String,
    key: &String,
    lock_id: &[u8],
    conn: &mut redis::aio::Connection,
    config: &Config,
) -> Result<i64, Box<dyn Error>> {
    if load_locked(pcr.clone(), key, conn).await?.eq(lock_id) {
        match delete_locked(pcr, key, conn).await {
            Ok(()) => {
                return Ok(config.operation_b_cost);
            }
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
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("pcr"),
            &String::from("test_store"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_load() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("pcr"),
            &String::from("test_load"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        let val = load(
            String::from("pcr"),
            &String::from("test_load"),
            &mut conn,
            &config,
        )
        .await?;
        assert_eq!(val.0, String::from("This is a test value"));
        Ok(())
    }

    #[tokio::test]
    async fn test_store_expiry() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("pcr"),
            &String::from("test_store_expiry"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        sleep(Duration::from_millis(1000));
        load(
            String::from("pcr"),
            &String::from("test_store_expiry"),
            &mut conn,
            &config,
        )
        .await
        .expect_err("should not load");
        Ok(())
    }

    #[tokio::test]
    async fn test_store_keepttl() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("pcr"),
            &String::from("test_store_keepttl"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        sleep(Duration::from_millis(400));
        store(
            String::from("pcr"),
            &String::from("test_store_keepttl"),
            -1,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        sleep(Duration::from_millis(400));
        load(
            String::from("pcr"),
            &String::from("test_store_keepttl"),
            &mut conn,
            &config,
        )
        .await?;
        sleep(Duration::from_millis(400));
        load(
            String::from("pcr"),
            &String::from("test_store_keepttl"),
            &mut conn,
            &config,
        )
        .await
        .expect_err("should not load");
        Ok(())
    }

    #[tokio::test]
    async fn test_store_zeroexpiry() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("pcr"),
            &String::from("test_store_zeroexpiry"),
            0,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await
        .expect_err("should not store zero expiry");
        Ok(())
    }
    #[tokio::test]
    async fn test_exists() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("pcr"),
            &String::from("test_exists"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        let check = exists(
            String::from("pcr"),
            &String::from("test_exists"),
            &mut conn,
            &config,
        )
        .await?;
        assert_eq!(true, check.0);
        let check = exists(
            String::from("pcr"),
            &String::from("not_in_db"),
            &mut conn,
            &config,
        )
        .await?;
        assert_eq!(false, check.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("pcr"),
            &String::from("test_delete"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        delete(
            String::from("pcr"),
            &String::from("test_delete"),
            &mut conn,
            &config,
        )
        .await?;
        let check = exists(
            String::from("pcr"),
            &String::from("test_delete"),
            &mut conn,
            &config,
        )
        .await?;
        assert_eq!(false, check.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_stat() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("pcr"),
            &String::from("test_stat"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        let info = stat(
            String::from("pcr"),
            &String::from("test_stat"),
            &mut conn,
            &config,
        )
        .await?;
        assert_eq!("test_stat", info.0.key);
        assert_eq!("This is a test value".len(), info.0.size);
        assert_eq!(true, info.0.is_terminal);
        Ok(())
    }

    #[tokio::test]
    async fn test_lock() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;

        lock(
            String::from("pcr"),
            &String::from("test_lock"),
            &mut conn,
            &config,
        )
        .await?;
        lock(
            String::from("pcr"),
            &String::from("test_lock"),
            &mut conn,
            &config,
        )
        .await
        .expect_err("lock not obtained");
        Ok(())
    }

    #[tokio::test]
    async fn test_lock_expiry() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;

        lock(
            String::from("pcr"),
            &String::from("test_lock_expiry"),
            &mut conn,
            &config,
        )
        .await?;
        sleep(Duration::from_millis(config.lock_expiry));
        lock(
            String::from("pcr"),
            &String::from("test_lock_expiry"),
            &mut conn,
            &config,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_unlock() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;

        let lock_id = lock(
            String::from("pcr"),
            &String::from("test_unlock"),
            &mut conn,
            &config,
        )
        .await?;
        unlock(
            String::from("pcr"),
            &String::from("test_unlock"),
            &lock_id.0,
            &mut conn,
            &config,
        )
        .await?;
        lock(
            String::from("pcr"),
            &String::from("test_unlock"),
            &mut conn,
            &config,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_list_recursive() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("pcr"),
            &String::from("test_list_recursive_0"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        store(
            String::from("pcr"),
            &String::from("test_list_recursive/1"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        store(
            String::from("pcr"),
            &String::from("test_list_recursive/2"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        store(
            String::from("pcr"),
            &String::from("unused_test_list_recursive"),
            1000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        let list_result = list(
            String::from("pcr"),
            &String::from("test_list_recursive"),
            true,
            &mut conn,
            &config,
        )
        .await?;
        assert_eq!(3, list_result.0.len());
        for i in &list_result.0 {
            print!("{}", i);
            if i.ne("test_list_recursive_0")
                && i.ne("test_list_recursive/1")
                && i.ne("test_list_recursive/2")
            {
                return Err("different key".into());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_store_benchmark() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;

        use std::time::Instant;
        let now = Instant::now();

        let mut i = 0;
        while i < 100000 {
            store(
                String::from("test_store_benchmark_namespace"),
                &String::from("test_store_benchmark_key"),
                1000,
                &String::from("This is a test value"),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        let elapsed = now.elapsed();
        println!("test_store_benchmark {} calls Elapsed: {:.2?}", i, elapsed);
        Ok(())
    }

    #[tokio::test]
    async fn test_load_benchmark() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        let mut i = 0;
        store(
            String::from("test_load_benchmark_namespace"),
            &(String::from("test_load_benchmark_key")),
            100000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        while i < 10000 {
            store(
                String::from("test_load_benchmark_namespace"),
                &(String::from("test_load_benchmark_key") + &i.to_string()),
                100000,
                &String::from("This is a test value"),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        use std::time::Instant;
        let now = Instant::now();

        let mut i = 0;
        while i < 100000 {
            let _val = load(
                String::from("test_load_benchmark_namespace"),
                &String::from("test_load_benchmark_key"),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        let elapsed = now.elapsed();
        println!("test_load_benchmark {} calls Elapsed: {:.2?}", i, elapsed);
        Ok(())
    }

    #[tokio::test]
    async fn test_exists_benchmark() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        let mut i = 0;
        store(
            String::from("test_exist_benchmark_namespace"),
            &(String::from("test_exist_benchmark_key")),
            100000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        while i < 10000 {
            store(
                String::from("test_exist_benchmark_namespace"),
                &(String::from("test_exist_benchmark_key") + &i.to_string()),
                100000,
                &String::from("This is a test value"),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        use std::time::Instant;
        let now = Instant::now();

        let mut i = 0;
        while i < 100000 {
            let _val = exists(
                String::from("test_exist_benchmark_namespace"),
                &String::from("test_exist_benchmark_key"),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        let elapsed = now.elapsed();
        println!("test_exists_benchmark {} calls Elapsed: {:.2?}", i, elapsed);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_benchmark() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        let mut i = 0;
        store(
            String::from("test_list_benchmark_namespace"),
            &(String::from("test_list_benchmark_key")),
            100000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        while i < 10000 {
            store(
                String::from("test_list_benchmark_namespace"),
                &(String::from("test_list_benchmark_key") + &i.to_string()),
                100000,
                &String::from("This is a test value"),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        use std::time::Instant;
        let now = Instant::now();

        let mut i = 0;
        while i < 100000 {
            let _val = list(
                String::from("test_list_benchmark_namespace"),
                &String::from("test_list_benchmark_key"),
                true,
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        let elapsed = now.elapsed();
        println!("test_lists_benchmark {} calls Elapsed: {:.2?}", i, elapsed);
        Ok(())
    }

    #[tokio::test]
    async fn test_stat_benchmark() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        store(
            String::from("test_stat_benchmark_namespace"),
            &(String::from("test_stat_benchmark_key")),
            100000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;

        use std::time::Instant;
        let now = Instant::now();

        let mut i = 0;
        while i < 100000 {
            let _val = stat(
                String::from("test_stat_benchmark_namespace"),
                &String::from("test_stat_benchmark_key"),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        let elapsed = now.elapsed();
        println!("test_stat_benchmark {} calls Elapsed: {:.2?}", i, elapsed);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_benchmark() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        let mut i = 0;
        store(
            String::from("test_delete_benchmark_namespace"),
            &(String::from("test_delete_benchmark_key")),
            100000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        while i < 100000 {
            store(
                String::from("test_delete_benchmark_namespace"),
                &(String::from("test_delete_benchmark_key") + &i.to_string()),
                100000,
                &String::from("This is a test value"),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        use std::time::Instant;
        let now = Instant::now();

        let mut i = 0;
        while i < 100000 {
            let _val = delete(
                String::from("test_delete_benchmark_namespace"),
                &(String::from("test_delete_benchmark_key") + &i.to_string()),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        let elapsed = now.elapsed();
        println!("test_delete_benchmark {} calls Elapsed: {:.2?}", i, elapsed);
        Ok(())
    }

    #[tokio::test]
    async fn test_lock_benchmark() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        let mut i = 0;
        store(
            String::from("test_lock_benchmark_namespace"),
            &(String::from("test_lock_benchmark_key")),
            100000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        while i < 100000 {
            store(
                String::from("test_lock_benchmark_namespace"),
                &(String::from("test_lock_benchmark_key") + &i.to_string()),
                100000,
                &String::from("This is a test value"),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        use std::time::Instant;
        let now = Instant::now();

        let mut i = 0;
        while i < 100000 {
            let _val = lock(
                String::from("test_lock_benchmark_namespace"),
                &(String::from("test_lock_benchmark_key") + &i.to_string()),
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        let elapsed = now.elapsed();
        println!("test_lock_benchmark {} calls Elapsed: {:.2?}", i, elapsed);
        Ok(())
    }

    #[tokio::test]
    async fn test_unlock_benchmark() -> Result<(), Box<dyn Error>> {
        let config: Config = Config::default();
        let mut conn = connect().await?;
        let mut i = 0;
        let mut lock_id: Vec<Vec<u8>>;
        lock_id = Vec::new();
        store(
            String::from("test_unlock_benchmark_namespace"),
            &(String::from("test_unlock_benchmark_key")),
            100000,
            &String::from("This is a test value"),
            &mut conn,
            &config,
        )
        .await?;
        while i < 100000 {
            store(
                String::from("test_unlock_benchmark_namespace"),
                &(String::from("test_unlock_benchmark_key") + &i.to_string()),
                100000,
                &String::from("This is a test value"),
                &mut conn,
                &config,
            )
            .await?;
            lock_id.push(
                lock(
                    String::from("test_unlock_benchmark_namespace"),
                    &(String::from("test_unlock_benchmark_key") + &i.to_string()),
                    &mut conn,
                    &config,
                )
                .await?
                .0,
            );
            i = i + 1;
        }

        use std::time::Instant;
        let now = Instant::now();

        let mut i = 0;
        while i < 100000 {
            let _val = unlock(
                String::from("test_unlock_benchmark_namespace"),
                &(String::from("test_unlock_benchmark_key") + &i.to_string()),
                &lock_id[i],
                &mut conn,
                &config,
            )
            .await?;
            i = i + 1;
        }

        let elapsed = now.elapsed();
        println!("test_unlock_benchmark {} calls Elapsed: {:.2?}", i, elapsed);
        Ok(())
    }
}
