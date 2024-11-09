use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Ok, Result};
use binance::futures::websockets::AGGREGATED_TRADE;
use chrono::{DateTime, Timelike};
use tokio::time::{self, Duration, Instant};

// the start is the start id of the missing segment
// this is to count the number of missing segments
// will be used to perform RESTFUL recovery after a certain number of missing segments
#[derive(Debug, Clone)]
pub struct Segment {
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug)]
pub enum AggTradeType {
    BIG,
    MIDDLE,
    SMALL,
    TINY,
}


#[derive(Debug)]
pub struct AtomicLock {
    lock: AtomicBool,
}

impl AtomicLock {
    pub fn new() -> Self {
        AtomicLock {
            lock: AtomicBool::new(false),
        }
    }

    pub fn test(&self) -> bool {
        self.lock.load(Ordering::SeqCst)
    }

    pub fn unlock(&self) {
        self.lock.store(false, Ordering::SeqCst);
    }

    pub fn lock(&self) {
        self.lock.store(true, Ordering::SeqCst);
    }

    pub async fn lock_and_wait_for_unlock(&self) {
        self.lock();
        // wait for someone releases the lock
        while self.lock.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    }
}

impl Segment {
    pub fn try_new(start: u64, end: u64) -> Result<Self, anyhow::Error> {
        if end < start {
            Err(anyhow::anyhow!("End must be greater than start"))
        } else {
            Ok(Segment { start, end })
        }
    }

    pub fn new(start: u64, end: u64) -> Self {
        Segment { start, end }
    }

    pub fn size(&self) -> u64 {
        self.end - self.start
    }
}

// returns the instant until and the timestamp of next time to send
pub async fn next_interval_time_point(interval: Duration) -> (Instant, u64) {
    let now = chrono::Local::now();
    let current_seconds = now.second() as u64;
    let interval_seconds = interval.as_secs();

    let next_interval_seconds = ((current_seconds / interval_seconds) + 1) * interval_seconds;
    let next_time_in_minute = next_interval_seconds % 60;
    // tracing::info!("next time in minute is {}", next_time_in_minute);
    let next_time: DateTime<chrono::Local> = now
        .with_second(next_time_in_minute as u32)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    // tracing::info!("now is {:?}, next time is {:?}", now, next_time);
    let duration_until_next_interval = now - next_time;
    tracing::info!(
        "duration until next interval is {:?}",
        duration_until_next_interval
    );
    (
        Instant::now()
            + Duration::from_secs(60 - (duration_until_next_interval.num_seconds() as u64))
            - Duration::from_millis(30),
        ((next_time.timestamp() / 1000) as u64) * 1000,
    )
}

pub fn next_aligned_instant(duration: Duration) -> (Instant, u64) {
    assert_eq!(
        60 % duration.as_secs(),
        0,
        "Duration must be divisible by 60"
    );

    let now = Instant::now();

    let now_since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    let period_secs = duration.as_secs();
    let next_aligned_secs = ((now_since_epoch / period_secs) + 1) * period_secs;
    let next_instant = Instant::now() + Duration::from_secs(next_aligned_secs - now_since_epoch);

    // 将Instant转换为对应的Unix时间戳
    let unix_timestamp = next_aligned_secs;

    (next_instant, unix_timestamp)
}

pub fn next_interval(seconds: u32) -> (Instant, u64) {
    assert!(
        seconds > 0 && seconds <= 60,
        "Seconds must be between 1 and 60"
    );
    assert!(60 % seconds == 0, "Seconds must be divisible by 60");

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let total_millis = now.as_millis(); // total milliseconds since UNIX epoch

    let next_target_sec = ((total_millis / 1000) / seconds as u128 + 1) * seconds as u128;
    let next_target_instant =
        Instant::now() + Duration::from_secs(next_target_sec as u64 - now.as_secs());

    let just_before_next_sec =
        Duration::from_millis((next_target_sec * 1000 - 1) as u64 - total_millis as u64);
    let just_before_next_instant = Instant::now() + just_before_next_sec;

    let just_before_next_timestamp = SystemTime::now() + just_before_next_sec;
    let just_before_next_timestamp_millis = just_before_next_timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    (next_target_instant, just_before_next_timestamp_millis)
}

pub fn get_next_instant_and_timestamp(seconds: u64) -> (Instant, u64) {
    assert!(
        seconds > 0 && seconds <= 60,
        "Seconds must be between 1 and 60"
    );
    assert!(60 % seconds == 0, "Seconds must be divisible by 60");

    // our first task is to calculate the current timestamp

    let now = SystemTime::now();

    let timestamp_millis = now
        .duration_since(UNIX_EPOCH)
        .expect("invalid timestamp")
        .as_millis();

    let milliseconds = seconds as u128 * 1000;
    let next_timestamp = (timestamp_millis / milliseconds + 1) * milliseconds;
    let diff = next_timestamp - timestamp_millis;
    let instant = Instant::now() + Duration::from_millis(diff as u64) + Duration::from_millis(5);
    (instant, next_timestamp as u64 - 1)
}

pub fn this_period_start(timestamp: u64, seconds: u32) -> u64 {
    assert!(
        seconds > 0 && seconds <= 60,
        "Seconds must be between 1 and 60"
    );
    assert!(60 % seconds == 0, "Seconds must be divisible by 60");
    (timestamp / (seconds as u64 * 1000)) * (seconds as u64 * 1000)
}

pub fn get_current_time() -> DateTime<chrono::Local> {
    chrono::Local::now()
}

pub struct CleanupTask<F>
where
    F: Fn() + Send + 'static,
{
    cleanup_action: Option<F>,
}

impl<F> CleanupTask<F>
where
    F: Fn() + Send + 'static,
{
    fn new(action: F) -> Self {
        CleanupTask {
            cleanup_action: Some(action),
        }
    }
}

impl<F> Drop for CleanupTask<F>
where
    F: Fn() + Send + 'static,
{
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup_action.take() {
            cleanup();
        }
    }
}

pub fn instant_from_timestamp(timestamp: u64) -> Instant {
    let now_instant = Instant::now();
    let now_system_time = SystemTime::now();

    let target_system_time = UNIX_EPOCH + Duration::from_secs(timestamp);

    let duration_until_target = match target_system_time.duration_since(now_system_time) {
        std::result::Result::Ok(duration) => duration,
        Err(_) => Duration::from_secs(0),
    };

    now_instant + duration_until_target
}

pub fn get_agg_trade_type(volume : f64) -> AggTradeType {
    let adjusted = volume - 1e-6;
    if adjusted > 1e4 {
        AggTradeType::BIG
    } else if adjusted > 1e3 {
        AggTradeType::MIDDLE
    } else if adjusted > 1e2 {
        AggTradeType::SMALL
    } else {
        AggTradeType::TINY
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::pause;

    #[tokio::test]
    async fn test_next_interval_15_seconds() {
        pause(); // Pause time for deterministic testing

        let duration = Duration::from_secs(15);
        let start = Instant::now();
        let next_time_point = next_interval_time_point(duration).await;
        let elapsed = next_time_point.duration_since(start);

        // Assert that the next time point is within the expected range (15-30 seconds)
        assert!(elapsed >= Duration::from_secs(15) && elapsed < Duration::from_secs(30));
    }

    #[tokio::test]
    async fn test_next_interval_30_seconds() {
        pause(); // Pause time for deterministic testing

        let duration = Duration::from_secs(30);
        let start = Instant::now();
        let next_time_point = next_interval_time_point(duration).await;
        let elapsed = next_time_point.duration_since(start);

        // Assert that the next time point is within the expected range (30-60 seconds)
        assert!(elapsed >= Duration::from_secs(30) && elapsed < Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_next_interval_60_seconds() {
        pause();

        let duration = Duration::from_secs(60);
        let start = Instant::now();
        let next_time_point = next_interval_time_point(duration).await;
        let elapsed = next_time_point.duration_since(start);

        // Assert that the next time point is within the expected range (60-120 seconds)
        assert!(elapsed >= Duration::from_secs(60) && elapsed < Duration::from_secs(120));
    }
}
