use crate::settings::Settings;
use actix_web::web::Data;
use anyhow::Result;
use log::error;
use moka::sync::Cache;
use r2d2::Pool;
use redis::{Client, ConnectionLike};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub enum RedisResult {
    Found(HashMap<String, String>), // 找到数据 -> 200
    NotFound,                       // 数据不存在 -> 404 (正常业务状态)
    ServiceError(String),           // Redis连接/服务问题 -> 500
}

pub trait ProjectLookup: Send + Sync {
    fn get_project(&self, appid: &str) -> RedisResult;
}

struct RedisProjectLookup {
    redis_pool: Data<Pool<Client>>,
    cache: Data<Cache<String, HashMap<String, String>>>,
}

impl ProjectLookup for RedisProjectLookup {
    fn get_project(&self, appid: &str) -> RedisResult {
        get_from_redis(
            format!("p:{appid}"),
            self.redis_pool.clone(),
            self.cache.clone(),
        )
    }
}

pub struct ProjectLookupState {
    lookup: Arc<dyn ProjectLookup>,
}

impl ProjectLookupState {
    pub fn from_redis(
        redis_pool: Data<Pool<Client>>,
        cache: Data<Cache<String, HashMap<String, String>>>,
    ) -> Self {
        Self::new(RedisProjectLookup { redis_pool, cache })
    }

    pub fn new<L: ProjectLookup + 'static>(lookup: L) -> Self {
        Self {
            lookup: Arc::new(lookup),
        }
    }

    pub fn get_project(&self, appid: &str) -> RedisResult {
        self.lookup.get_project(appid)
    }
}

impl RedisResult {
    pub fn is_found(&self) -> bool {
        matches!(self, RedisResult::Found(_))
    }

    pub fn into_data(self) -> Option<HashMap<String, String>> {
        match self {
            RedisResult::Found(data) => Some(data),
            _ => None,
        }
    }
}

pub fn get_from_redis_ref(
    key: &str,
    redis_pool: &Data<Pool<Client>>,
    cache: &Data<Cache<String, HashMap<String, String>>>,
) -> RedisResult {
    // 首先检查缓存
    if cache.contains_key(key) {
        if let Some(data) = cache.get(key) {
            return RedisResult::Found(data);
        }
        // 缓存中的数据可能在contains_key和get之间被LRU驱逐，这是正常现象
        // 继续从Redis获取即可
    }

    // 缓存中没有，从Redis获取
    let connection_result = redis_pool.get();
    let mut connection = match connection_result {
        Ok(conn) => conn,
        Err(err) => {
            error!("Failed to get Redis connection from pool: {err}");
            return RedisResult::ServiceError(format!("Connection pool error: {err}"));
        }
    };

    get_from_connection_ref(key, &mut *connection, cache)
}

fn get_from_connection_ref<C: ConnectionLike>(
    key: &str,
    connection: &mut C,
    cache: &Data<Cache<String, HashMap<String, String>>>,
) -> RedisResult {
    let hgetall_result: Result<HashMap<String, String>, redis::RedisError> =
        redis::cmd("HGETALL").arg(key).query(connection);

    match hgetall_result {
        Ok(shorten_map) => {
            if shorten_map.is_empty() {
                RedisResult::NotFound
            } else {
                cache.insert(key.to_string(), shorten_map.clone());
                RedisResult::Found(shorten_map)
            }
        }
        Err(err) => {
            error!("Redis hgetall failed for key {key}: {err}");
            RedisResult::ServiceError(format!("Redis operation failed: {err}"))
        }
    }
}

pub fn get_from_redis(
    key: String,
    redis_pool: Data<Pool<Client>>,
    cache: Data<Cache<String, HashMap<String, String>>>,
) -> RedisResult {
    get_from_redis_ref(&key, &redis_pool, &cache)
}
pub fn init_redis_client(settings: &Settings) -> Data<Pool<Client>> {
    let redis_settings = settings
        .redis
        .as_ref()
        .expect("redis client initialization requires [redis] config");
    let redis_client = Client::open(redis_settings.address.clone()).unwrap();
    let redis_pool = Pool::builder()
        .max_size(redis_settings.connections_max_size)
        .min_idle(redis_settings.connections_min_size)
        .build(redis_client)
        .unwrap();

    Data::new(redis_pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::RedisError;
    use redis_test::{MockCmd, MockRedisConnection};
    use std::io;

    fn new_cache() -> Data<Cache<String, HashMap<String, String>>> {
        Data::new(Cache::builder().initial_capacity(10).build())
    }

    #[test]
    fn get_from_connection_ref_returns_found_and_populates_cache() {
        let cache = new_cache();
        let key = "p:APPID";
        let mut conn = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("HGETALL").arg(key),
            Ok(HashMap::from([
                ("os".to_string(), "android".to_string()),
                ("re_attribution".to_string(), "300".to_string()),
            ])),
        )]);

        let result = get_from_connection_ref(key, &mut conn, &cache);

        match result {
            RedisResult::Found(data) => {
                assert_eq!(data.get("os"), Some(&"android".to_string()));
                assert_eq!(data.get("re_attribution"), Some(&"300".to_string()));
            }
            _ => panic!("expected found"),
        }
        assert!(cache.contains_key(key));
    }

    #[test]
    fn get_from_connection_ref_returns_not_found_for_empty_hash() {
        let cache = new_cache();
        let key = "p:MISSING";
        let mut conn = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("HGETALL").arg(key),
            Ok(HashMap::<String, String>::new()),
        )]);

        let result = get_from_connection_ref(key, &mut conn, &cache);

        assert!(matches!(result, RedisResult::NotFound));
        assert!(!cache.contains_key(key));
    }

    #[test]
    fn get_from_connection_ref_maps_redis_error_to_service_error() {
        let cache = new_cache();
        let key = "p:ERROR";
        let err = RedisError::from(io::Error::other("boom"));
        let mut conn = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("HGETALL").arg(key),
            Err::<HashMap<String, String>, _>(err),
        )]);

        let result = get_from_connection_ref(key, &mut conn, &cache);

        assert!(matches!(result, RedisResult::ServiceError(_)));
        assert!(!cache.contains_key(key));
    }
}
