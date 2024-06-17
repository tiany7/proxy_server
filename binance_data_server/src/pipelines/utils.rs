use std::sync::Arc;
use tokio::sync::{RwLock as AsyncRwLock, Mutex as AsyncMutex, Notify};
use tokio::task;

pub struct ReadPriorityRwLock<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    data: AsyncRwLock<T>,
    read_count: AsyncMutex<u32>,
    notify: Notify,
}

impl<T> ReadPriorityRwLock<T> {
    pub fn new(data: T) -> Self {
        ReadPriorityRwLock {
            inner: Arc::new(Inner {
                data: AsyncRwLock::new(data),
                read_count: AsyncMutex::new(0),
                notify: Notify::new(),
            }),
        }
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, T> {
        let mut read_count = self.inner.read_count.lock().await;
        *read_count += 1;
        drop(read_count);

        let guard = self.inner.data.read().await;

        {
            let mut read_count = self.inner.read_count.lock().await;
            *read_count -= 1;
            if *read_count == 0 {
                self.inner.notify.notify_one();
            }
        }

        guard
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, T> {
        loop {
            let read_count = self.inner.read_count.lock().await;
            if *read_count == 0 {
                drop(read_count);
                break;
            }
            drop(read_count);
            self.inner.notify.notified().await;
        }

        self.inner.data.write().await
    }
}
