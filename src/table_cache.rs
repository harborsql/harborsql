use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use datafusion::catalog::TableProvider;
use hmac::{Hmac, Mac};
use object_store::ObjectStore;
use sha2::Sha256;
use tokio::sync::Notify;
use url::Url;
use uuid::Uuid;

use crate::error::{HarborError, Result};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct TableCache {
    inner: Arc<TableCacheInner>,
}

struct TableCacheInner {
    entries: Mutex<HashMap<TableCacheKey, Arc<EntrySlot>>>,
    max_entries: usize,
    ttl: Duration,
    fingerprint_key: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct TableCacheKey {
    principal_fingerprint: [u8; 32],
    full_name: String,
    aws_region: String,
}

struct EntrySlot {
    state: Mutex<EntryState>,
    notify: Notify,
}

enum EntryState {
    Empty,
    Loading,
    Ready(Arc<CachedTable>),
}

pub struct CachedTable {
    pub provider: Arc<dyn TableProvider>,
    pub object_store_url: Url,
    pub object_prefix: String,
    pub object_store: Arc<dyn ObjectStore>,
    expires_at: Instant,
}

impl TableCache {
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        let mut fingerprint_key = [0_u8; 32];
        fingerprint_key[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        fingerprint_key[16..].copy_from_slice(Uuid::new_v4().as_bytes());
        Self::with_fingerprint_key(max_entries, ttl, fingerprint_key)
    }

    fn with_fingerprint_key(max_entries: usize, ttl: Duration, fingerprint_key: [u8; 32]) -> Self {
        Self {
            inner: Arc::new(TableCacheInner {
                entries: Mutex::new(HashMap::new()),
                max_entries,
                ttl,
                fingerprint_key,
            }),
        }
    }

    pub async fn get_or_load<F, Fut>(
        &self,
        bearer_token: &str,
        full_name: &str,
        aws_region: &str,
        load: F,
    ) -> Result<Arc<CachedTable>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CachedTable>>,
    {
        if !self.is_enabled() {
            return load().await.map(Arc::new);
        }

        let key = self.cache_key(bearer_token, full_name, aws_region)?;
        let mut load = Some(load);

        loop {
            let Some(slot) = self.slot_for_key(key.clone(), Instant::now())? else {
                let load = load.take().ok_or_else(|| {
                    HarborError::Query("table cache loader was consumed unexpectedly".into())
                })?;
                return load().await.map(Arc::new);
            };

            let notified = slot.notify.notified();
            let should_wait;
            {
                let mut state = lock(&slot.state)?;
                match &*state {
                    EntryState::Ready(cached) if !cached.is_expired(Instant::now()) => {
                        return Ok(cached.clone());
                    }
                    EntryState::Loading => {
                        should_wait = true;
                    }
                    EntryState::Ready(_) | EntryState::Empty => {
                        *state = EntryState::Loading;
                        should_wait = false;
                    }
                }
            }

            if should_wait {
                notified.await;
                continue;
            }
            drop(notified);

            let load = load.take().ok_or_else(|| {
                HarborError::Query("table cache loader was consumed unexpectedly".into())
            })?;
            let loaded = load().await;
            return self.finish_load(key, slot, loaded);
        }
    }

    fn is_enabled(&self) -> bool {
        self.inner.max_entries > 0 && !self.inner.ttl.is_zero()
    }

    fn cache_key(
        &self,
        bearer_token: &str,
        full_name: &str,
        aws_region: &str,
    ) -> Result<TableCacheKey> {
        Ok(TableCacheKey {
            principal_fingerprint: self.principal_fingerprint(bearer_token)?,
            full_name: full_name.to_string(),
            aws_region: aws_region.to_string(),
        })
    }

    fn principal_fingerprint(&self, bearer_token: &str) -> Result<[u8; 32]> {
        let mut mac = HmacSha256::new_from_slice(&self.inner.fingerprint_key).map_err(|err| {
            HarborError::Query(format!("invalid table cache fingerprint key: {err}"))
        })?;
        mac.update(bearer_token.as_bytes());
        let bytes = mac.finalize().into_bytes();
        let mut fingerprint = [0_u8; 32];
        fingerprint.copy_from_slice(&bytes);
        Ok(fingerprint)
    }

    fn slot_for_key(&self, key: TableCacheKey, now: Instant) -> Result<Option<Arc<EntrySlot>>> {
        let mut entries = lock(&self.inner.entries)?;
        prune_expired_entries(&mut entries, now)?;

        if let Some(slot) = entries.get(&key) {
            return Ok(Some(slot.clone()));
        }

        if entries.len() >= self.inner.max_entries {
            return Ok(None);
        }

        let slot = Arc::new(EntrySlot {
            state: Mutex::new(EntryState::Empty),
            notify: Notify::new(),
        });
        entries.insert(key, slot.clone());
        Ok(Some(slot))
    }

    fn finish_load(
        &self,
        key: TableCacheKey,
        slot: Arc<EntrySlot>,
        loaded: Result<CachedTable>,
    ) -> Result<Arc<CachedTable>> {
        match loaded {
            Ok(mut cached) => {
                let now = Instant::now();
                cached.cap_expires_at(checked_deadline(now, self.inner.ttl));
                let cached = Arc::new(cached);
                if cached.is_expired(now) {
                    *lock(&slot.state)? = EntryState::Empty;
                    self.remove_slot(&key, &slot)?;
                } else {
                    *lock(&slot.state)? = EntryState::Ready(cached.clone());
                }
                slot.notify.notify_waiters();
                Ok(cached)
            }
            Err(err) => {
                *lock(&slot.state)? = EntryState::Empty;
                self.remove_slot(&key, &slot)?;
                slot.notify.notify_waiters();
                Err(err)
            }
        }
    }

    fn remove_slot(&self, key: &TableCacheKey, slot: &Arc<EntrySlot>) -> Result<()> {
        let mut entries = lock(&self.inner.entries)?;
        if entries
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            entries.remove(key);
        }
        Ok(())
    }
}

impl CachedTable {
    pub fn new(
        provider: Arc<dyn TableProvider>,
        object_store_url: Url,
        object_prefix: String,
        object_store: Arc<dyn ObjectStore>,
        expires_at: Instant,
    ) -> Self {
        Self {
            provider,
            object_store_url,
            object_prefix,
            object_store,
            expires_at,
        }
    }

    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at <= now
    }

    fn cap_expires_at(&mut self, max_expires_at: Instant) {
        self.expires_at = self.expires_at.min(max_expires_at);
    }
}

pub fn expires_at_from_unity_expiration_ms(
    expiration_time_ms: i64,
    safety_skew: Duration,
) -> Instant {
    expires_at_from_unity_expiration_ms_at(
        expiration_time_ms,
        safety_skew,
        SystemTime::now(),
        Instant::now(),
    )
}

fn expires_at_from_unity_expiration_ms_at(
    expiration_time_ms: i64,
    safety_skew: Duration,
    now_system: SystemTime,
    now_instant: Instant,
) -> Instant {
    let Ok(expiration_time_ms) = u64::try_from(expiration_time_ms) else {
        return now_instant;
    };
    let credential_expiry = UNIX_EPOCH + Duration::from_millis(expiration_time_ms);
    let remaining = credential_expiry
        .duration_since(now_system)
        .unwrap_or(Duration::ZERO)
        .saturating_sub(safety_skew);
    checked_deadline(now_instant, remaining)
}

fn checked_deadline(now: Instant, duration: Duration) -> Instant {
    now.checked_add(duration).unwrap_or(now)
}

fn prune_expired_entries(
    entries: &mut HashMap<TableCacheKey, Arc<EntrySlot>>,
    now: Instant,
) -> Result<()> {
    let mut expired = Vec::new();
    for (key, slot) in entries.iter() {
        let state = lock(&slot.state)?;
        match &*state {
            EntryState::Empty => expired.push(key.clone()),
            EntryState::Ready(cached) if cached.is_expired(now) => expired.push(key.clone()),
            EntryState::Loading | EntryState::Ready(_) => {}
        }
    }
    for key in expired {
        entries.remove(&key);
    }
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| HarborError::Query("table cache lock was poisoned".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::{arrow::datatypes::Schema, datasource::empty::EmptyTable};
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn coalesces_concurrent_loads_for_same_principal_and_table() {
        let cache = TableCache::with_fingerprint_key(16, Duration::from_secs(60), [7; 32]);
        let loads = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();

        for _ in 0..10 {
            let cache = cache.clone();
            let loads = loads.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_load(
                        "token-a",
                        "workspace.default.hits",
                        "us-west-2",
                        || async move {
                            loads.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(25)).await;
                            Ok(test_cached_table(Instant::now() + Duration::from_secs(60)))
                        },
                    )
                    .await
                    .unwrap();
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn does_not_share_entries_across_bearer_tokens() {
        let cache = TableCache::with_fingerprint_key(16, Duration::from_secs(60), [7; 32]);
        let loads = Arc::new(AtomicUsize::new(0));

        for token in ["token-a", "token-b"] {
            let loads = loads.clone();
            cache
                .get_or_load(
                    token,
                    "workspace.default.hits",
                    "us-west-2",
                    || async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        Ok(test_cached_table(Instant::now() + Duration::from_secs(60)))
                    },
                )
                .await
                .unwrap();
        }

        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn subtracts_safety_skew_from_unity_credential_expiration() {
        let now_system = UNIX_EPOCH + Duration::from_millis(10_000);
        let now_instant = Instant::now();
        let expires_at = expires_at_from_unity_expiration_ms_at(
            20_000,
            Duration::from_secs(3),
            now_system,
            now_instant,
        );

        assert!(expires_at >= now_instant + Duration::from_secs(6));
        assert!(expires_at <= now_instant + Duration::from_secs(7));
    }

    fn test_cached_table(expires_at: Instant) -> CachedTable {
        CachedTable::new(
            Arc::new(EmptyTable::new(Arc::new(Schema::empty()))),
            Url::parse("memory:///").unwrap(),
            String::new(),
            Arc::new(InMemory::new()),
            expires_at,
        )
    }
}
