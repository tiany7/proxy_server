use redis::AsyncCommands;
use prost::Message;


#[derive(Debug, Clone)]
struct RedisClient {
    name : String,
    client: redis::Client,
}

impl RedisClient {
    pub fn new(name: &str, url: &str) -> Self {
        let client = redis::Client::open(url).unwrap();
        RedisClient {
            name: name.to_string(),
            client,
        }
    }

    pub async fn get(&self, key: &str) -> redis::RedisResult<String> {
        let mut conn = self.client.get_async_connection().await.unwrap();
        conn.get(key).await
    }

    pub async fn set(&self, key: &str, value: &str) -> redis::RedisResult<String> {
        let mut conn = self.client.get_async_connection().await.unwrap();
        conn.set(key, value).await
    }

    // key must be proto message
    pub async fn append_timely_bar_data<M: Message>(&self, key: &str, value: M, timestamp: u64) -> redis::RedisResult<()> {
        let mut conn = self.client.get_async_connection().await.unwrap();
        let mut buf = Vec::new();
        proto_message.encode(&mut buf).unwrap();
        conn.zadd(key, (timestamp, buf)).await?;
    }

    pub async fn get_timely_bar_data<M: Message>(&self, key: &str, start: u64, end: u64) -> redis::RedisResult<Vec<M>> {
        let mut conn = self.client.get_async_connection().await.unwrap();
        let result: Vec<Vec<u8>> = conn.zrangebyscore(key, start, end).await?;
        let mut messages = result.iter().map(|x| {
             M::default().merge(&mut x.as_slice()).unwrap()
        }).collect();
        Ok(messages)
    }
}