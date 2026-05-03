use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use rand::Rng;
use serde::Deserialize;
use tokio::sync::Mutex;

use super::auth::TelegramSessionHandle;
use super::parallel_download;

pub struct StreamServerInfo {
    pub port: u16,
    pub token: String,
}

#[derive(Clone)]
struct ServerState {
    session: TelegramSessionHandle,
    token: Arc<String>,
}

#[derive(Deserialize)]
struct StreamQuery {
    token: Option<String>,
}

fn generate_token() -> String {
    let bytes: [u8; 24] = rand::thread_rng().gen();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn parse_range_header(header: Option<&HeaderValue>, total_size: u64) -> (u64, u64, bool) {
    let Some(value) = header.and_then(|v| v.to_str().ok()) else {
        return (0, total_size.saturating_sub(1), false);
    };
    let stripped = match value.strip_prefix("bytes=") {
        Some(s) => s,
        None => return (0, total_size.saturating_sub(1), false),
    };
    let first = stripped.split(',').next().unwrap_or("").trim();
    let mut parts = first.splitn(2, '-');
    let start_str = parts.next().unwrap_or("");
    let end_str = parts.next().unwrap_or("");
    let total_minus_one = total_size.saturating_sub(1);
    let (start, end) = match (start_str.is_empty(), end_str.is_empty()) {
        (true, false) => {
            let suffix: u64 = end_str.parse().unwrap_or(0);
            let s = total_size.saturating_sub(suffix);
            (s, total_minus_one)
        }
        (false, true) => {
            let s: u64 = start_str.parse().unwrap_or(0);
            (s.min(total_minus_one), total_minus_one)
        }
        (false, false) => {
            let s: u64 = start_str.parse().unwrap_or(0);
            let e: u64 = end_str.parse().unwrap_or(total_minus_one);
            (s.min(total_minus_one), e.min(total_minus_one))
        }
        _ => (0, total_minus_one),
    };
    (start, end, true)
}

async fn handle_stream(
    State(state): State<ServerState>,
    Path((chat_type, chat_id, message_id)): Path<(String, i64, i32)>,
    Query(q): Query<StreamQuery>,
    headers: HeaderMap,
) -> Response {
    if q.token.as_deref() != Some(state.token.as_str()) {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::from("invalid token"))
            .unwrap();
    }

    let (client, access_hash) = {
        let guard = state.session.lock().await;
        let Some(client) = guard.client.clone() else {
            return Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Body::from("not authenticated"))
                .unwrap();
        };
        let access_hash = guard.peer_hashes.get(&chat_id).copied().unwrap_or(0);
        (client, access_hash)
    };

    let is_channel =
        chat_type == "channel" || (chat_type == "group" && access_hash != 0);

    let (raw_media, _date) = match parallel_download::fetch_raw_media(
        &client, chat_id, access_hash, is_channel, message_id,
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(format!("fetch_raw_media: {}", e)))
                .unwrap();
        }
    };

    let media_loc = match parallel_download::media_to_location(&raw_media) {
        Some(m) => m,
        None => {
            return Response::builder()
                .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                .body(Body::from("unsupported media"))
                .unwrap();
        }
    };

    let mime = parallel_download::mime_type_from_media(&raw_media);
    let total_size = media_loc.size;
    let etag = format!("\"tg-{}-{}-{}\"", chat_id, message_id, total_size);

    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        if if_none_match == etag {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header(header::ETAG, &etag)
                .header(header::CACHE_CONTROL, "private, max-age=86400, immutable")
                .header("access-control-allow-origin", "*")
                .body(Body::empty())
                .unwrap();
        }
    }

    let (start, end, has_range) = parse_range_header(headers.get(header::RANGE), total_size);
    let chunk_len = end.saturating_sub(start).saturating_add(1);

    let stream = parallel_download::stream_file_range(client, media_loc, start, end);
    let body = Body::from_stream(stream);

    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, &mime)
        .header(header::CONTENT_LENGTH, chunk_len.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, "private, max-age=86400, immutable")
        .header(header::ETAG, &etag)
        .header(header::LAST_MODIFIED, "Thu, 01 Jan 1970 00:00:00 GMT")
        .header("access-control-allow-origin", "*")
        .header("access-control-allow-methods", "GET, HEAD, OPTIONS")
        .header("access-control-allow-headers", "*");

    if has_range {
        builder = builder
            .status(StatusCode::PARTIAL_CONTENT)
            .header(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", start, end, total_size),
            );
    } else {
        builder = builder.status(StatusCode::OK);
    }

    builder.body(body).unwrap()
}

async fn handle_options() -> Response {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("access-control-allow-origin", "*")
        .header("access-control-allow-methods", "GET, HEAD, OPTIONS")
        .header("access-control-allow-headers", "*")
        .body(Body::empty())
        .unwrap()
}

pub async fn start(
    session: TelegramSessionHandle,
) -> anyhow::Result<Arc<Mutex<StreamServerInfo>>> {
    let token = generate_token();
    let state = ServerState {
        session,
        token: Arc::new(token.clone()),
    };

    let app = Router::new()
        .route(
            "/stream/:chat_type/:chat_id/:message_id",
            get(handle_stream).options(handle_options),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(SocketAddr::from((
        [127, 0, 0, 1],
        0,
    )))
    .await?;
    let port = listener.local_addr()?.port();
    tracing::info!("[tg-stream] streaming server listening on 127.0.0.1:{}", port);

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("[tg-stream] server error: {}", e);
        }
    });

    tokio::spawn(async move {
        let tracker = super::parallel_download::global_stream_tracker().clone();
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let rate = tracker.current_rate_bytes_per_sec().await;
            if rate > 0 {
                tracing::debug!(
                    "[tg-stream] speed={:.2} MB/s ({} B/s)",
                    rate as f64 / (1024.0 * 1024.0),
                    rate
                );
            }
        }
    });

    Ok(Arc::new(Mutex::new(StreamServerInfo { port, token })))
}
