use prost::Message;
use redis::AsyncCommands;

use crate::pipelines::pipelines::trade::BarData;

#[derive(Debug, Clone)]
pub struct RedisClient {
    name: String,
    client: redis::Client,
}

// update the aggregated trade data to the bar data
pub fn update_agg_trade(old_bar: &mut BarData, new_bar: &BarData) {
    old_bar.volume += new_bar.volume;
    old_bar.high = old_bar.high.max(new_bar.high);
    old_bar.low = old_bar.low.min(new_bar.low);
    // old_bar.close = new_bar.close
    old_bar.taker_buy_base_asset_volume += new_bar.taker_buy_base_asset_volume;
    old_bar.taker_buy_quote_asset_volume += new_bar.taker_buy_quote_asset_volume;
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
    pub async fn append_timely_bar_data<M: Message + std::default::Default>(
        &self,
        key: &str,
        value: M,
        timestamp: u64,
    ) -> redis::RedisResult<()> {
        let mut conn = self.client.get_multiplexed_async_connection ().await.unwrap();
        let mut buf = Vec::new();
        value.encode(&mut buf).unwrap();
        conn.zadd::<&str, u64, std::vec::Vec<u8>, ()>(key, buf, timestamp).await;
        Ok(())
    }

    pub async fn get_timely_bar_data<M: Message + std::default::Default>(
        &self,
        key: &str,
        start: u64,
        end: u64,
    ) -> redis::RedisResult<Vec<M>> {
        let mut conn = self.client.get_multiplexed_async_connection().await.unwrap();
        let result: Vec<Vec<u8>> = conn.zrangebyscore(key, start, end).await?;
        let mut messages = result
            .iter()
            .map(|x| {
                let mut m = M::default();
                m.merge(&mut x.as_slice()).unwrap();
                m
            })
            .collect();
        Ok(messages)
    }

    pub async fn modify_set_value(
        &self,
        zset_key: &str,
        score: f64,
        new_bardata: BarData,
    ) -> redis::RedisResult<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await.unwrap();

        let existing_data: Option<Vec<u8>> = conn
            .zrangebyscore_limit(zset_key, score, score, 0, 1)
            .await?;

        if let Some(data) = existing_data {
            let mut old_bardata = BarData::decode(&*data).unwrap();

            update_agg_trade(&mut old_bardata, &new_bardata);
            let mut buf = Vec::new();
            old_bardata.encode(&mut buf).unwrap();

            conn.zrem(zset_key, data).await?;
            conn.zadd(zset_key, buf, score).await?;

            Ok(())
        } else {
            tracing::warn!("No data found for score {}", score);
            Ok(())
        }
    }
}
