use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use omniget_plugin_sdk::PluginHost;
use serde::Serialize;
use tokio::sync::Mutex as TokioMutex;

const PROGRESS_THROTTLE_MS: u64 = 250;

static EXTERNAL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn next_queue_id() -> u64 {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let bump = EXTERNAL_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    now_ms.wrapping_mul(1000).wrapping_add(bump & 0xFFFF)
}

fn throttle_map() -> &'static Arc<TokioMutex<HashMap<u64, Instant>>> {
    use std::sync::OnceLock;
    static MAP: OnceLock<Arc<TokioMutex<HashMap<u64, Instant>>>> = OnceLock::new();
    MAP.get_or_init(|| Arc::new(TokioMutex::new(HashMap::new())))
}

#[derive(Serialize)]
struct EnqueuePayload<'a> {
    queue_id: u64,
    platform: &'a str,
    title: &'a str,
    output_path: &'a Path,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thumbnail_url: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_name: Option<&'a str>,
}

pub fn enqueue_external(
    host: &dyn PluginHost,
    platform: &str,
    title: &str,
    output_path: &Path,
    total_bytes: Option<u64>,
    kind: Option<&str>,
    thumbnail_url: Option<&str>,
    url: Option<&str>,
    file_name: Option<&str>,
) -> u64 {
    let queue_id = next_queue_id();
    let payload = EnqueuePayload {
        queue_id,
        platform,
        title,
        output_path,
        total_bytes,
        thumbnail_url,
        kind,
        url,
        file_name,
    };
    if let Ok(value) = serde_json::to_value(&payload) {
        let _ = host.emit_event("host:queue:external:enqueue", value);
        tracing::info!("[tg-host-queue] enqueue queue_id={} platform={} kind={:?}", queue_id, platform, kind);
    }
    queue_id
}

#[derive(Serialize)]
struct ProgressPayload {
    queue_id: u64,
    percent: f64,
    downloaded_bytes: u64,
    speed_bytes_per_sec: f64,
}

pub async fn report_progress(
    host: &dyn PluginHost,
    queue_id: u64,
    percent: f64,
    downloaded_bytes: u64,
    speed_bytes_per_sec: f64,
) {
    let now = Instant::now();
    let should_emit = {
        let mut m = throttle_map().lock().await;
        match m.get(&queue_id) {
            Some(last) if now.duration_since(*last) < Duration::from_millis(PROGRESS_THROTTLE_MS) => {
                false
            }
            _ => {
                m.insert(queue_id, now);
                true
            }
        }
    };
    if !should_emit {
        return;
    }
    let payload = ProgressPayload {
        queue_id,
        percent: percent.clamp(0.0, 100.0),
        downloaded_bytes,
        speed_bytes_per_sec,
    };
    if let Ok(value) = serde_json::to_value(&payload) {
        let _ = host.emit_event("host:queue:external:progress", value);
    }
}

#[derive(Serialize)]
struct CompletePayload<'a> {
    queue_id: u64,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_path: Option<&'a Path>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_size_bytes: Option<u64>,
}

pub async fn report_complete(
    host: &dyn PluginHost,
    queue_id: u64,
    success: bool,
    file_path: Option<&Path>,
    error: Option<&str>,
    file_size_bytes: Option<u64>,
) {
    let payload = CompletePayload {
        queue_id,
        success,
        file_path,
        error,
        file_size_bytes,
    };
    if let Ok(value) = serde_json::to_value(&payload) {
        let _ = host.emit_event("host:queue:external:complete", value);
        tracing::info!("[tg-host-queue] complete queue_id={} success={}", queue_id, success);
    }
    {
        let mut m = throttle_map().lock().await;
        m.remove(&queue_id);
    }
}
