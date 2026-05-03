pub mod platforms;
pub mod host_queue;
pub mod preview_cache;

use std::collections::HashMap;
use std::sync::Arc;
use serde::Serialize;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use base64::Engine;
use omniget_plugin_sdk::{OmnigetPlugin, PluginHost};
use platforms::telegram::api::{self};
use platforms::telegram::auth::{self, TelegramSessionHandle, TelegramState, QrPollStatus, VerifyError};
use platforms::telegram::stream_server::{self, StreamServerInfo};
use platforms::telegram::thumbnail_cache;

pub struct TelegramPlugin {
    host: Option<Arc<dyn PluginHost>>,
    runtime: Arc<tokio::runtime::Runtime>,
    telegram_session: TelegramSessionHandle,
    active_downloads: Arc<tokio::sync::Mutex<HashMap<u64, (String, CancellationToken)>>>,
    stream_server: Arc<tokio::sync::Mutex<Option<Arc<tokio::sync::Mutex<StreamServerInfo>>>>>,
}

impl TelegramPlugin {
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(8 * 1024 * 1024)
            .build()
            .expect("Failed to create tokio runtime for telegram plugin");
        Self {
            host: None,
            runtime: Arc::new(runtime),
            telegram_session: Arc::new(tokio::sync::Mutex::new(TelegramState::new())),
            active_downloads: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            stream_server: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    fn get_setting(&self, key: &str) -> serde_json::Value {
        self.host.as_ref()
            .map(|h| h.get_settings("telegram"))
            .and_then(|s| s.get(key).cloned())
            .unwrap_or(serde_json::Value::Null)
    }

    fn fix_file_extensions(&self) -> bool {
        self.get_setting("fix_file_extensions").as_bool().unwrap_or(false)
    }

    fn concurrent_downloads(&self) -> usize {
        self.get_setting("concurrent_downloads").as_u64().unwrap_or(3) as usize
    }
}

#[derive(Clone, Serialize)]
struct GenericDownloadProgress { id: u64, title: String, platform: String, percent: f64 }

#[derive(Clone, Serialize)]
struct GenericDownloadComplete { id: u64, title: String, platform: String, success: bool, error: Option<String>, file_path: Option<String>, file_size_bytes: Option<u64>, file_count: Option<u32>, #[serde(skip_serializing_if = "Option::is_none")] deleted_count: Option<u32> }

#[derive(Clone, Serialize)]
struct GenericUploadProgress { id: u64, title: String, platform: String, percent: f64 }

#[derive(Clone, Serialize)]
struct GenericUploadComplete { id: u64, title: String, platform: String, success: bool, error: Option<String>, message_id: Option<i32> }

#[derive(Clone, Serialize)]
struct BatchFileStatus { batch_id: u64, message_id: i32, status: String, percent: f64, error: Option<String> }

#[derive(Clone, serde::Deserialize)]
struct BatchItem { message_id: i32, file_name: String, file_size: u64 }

impl OmnigetPlugin for TelegramPlugin {
    fn id(&self) -> &str { "telegram" }
    fn name(&self) -> &str { "Telegram Downloader" }
    fn version(&self) -> &str { env!("CARGO_PKG_VERSION") }

    fn initialize(&mut self, host: Arc<dyn PluginHost>) -> anyhow::Result<()> {
        if let Some(proxy) = host.proxy_config() {
            omniget_core::core::http_client::init_proxy(
                omniget_core::models::settings::ProxySettings {
                    enabled: true,
                    proxy_type: proxy.proxy_type,
                    host: proxy.host,
                    port: proxy.port,
                    username: proxy.username.unwrap_or_default(),
                    password: proxy.password.unwrap_or_default(),
                }
            );
        }
        let preview_dir = host.plugin_data_dir("telegram").join("previews");
        platforms::telegram::thumbnail_cache::init_disk_cache(preview_dir);

        let saved = host.get_settings("telegram");
        if let Some(n) = saved.get("max_threads").and_then(|v| v.as_u64()) {
            platforms::telegram::parallel_download::set_max_threads_cap(n as usize);
        }
        if let Some(v) = saved.get("sync_enabled").and_then(|v| v.as_bool()) {
            platforms::telegram::sync_state::set_enabled(v);
        }
        if let Some(n) = saved.get("sync_interval_min").and_then(|v| v.as_u64()) {
            platforms::telegram::sync_state::set_interval_min(n as u32);
        }

        self.host = Some(host.clone());

        let session = self.telegram_session.clone();
        let server_slot = self.stream_server.clone();
        self.runtime.spawn(async move {
            match stream_server::start(session).await {
                Ok(info) => {
                    let mut slot = server_slot.lock().await;
                    *slot = Some(info);
                }
                Err(e) => {
                    tracing::error!("[tg-stream] failed to start streaming server: {}", e);
                }
            }
        });

        let sync_session = self.telegram_session.clone();
        let sync_host = host.clone();
        let sync_cancel = tokio_util::sync::CancellationToken::new();
        platforms::telegram::sync_state::spawn_background_loop(
            sync_session,
            sync_host,
            self.runtime.handle().clone(),
            sync_cancel,
        );

        Ok(())
    }

    fn handle_command(
        &self,
        command: String,
        args: serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'static>> {
        let host = self.host.clone();
        let session = self.telegram_session.clone();
        let active = self.active_downloads.clone();
        let stream_server_slot = self.stream_server.clone();
        let fix_ext = self.fix_file_extensions();
        let concurrent = self.concurrent_downloads().clamp(1, 10);
        let runtime_handle = self.runtime.handle().clone();

        Box::pin(async move {
            runtime_handle.spawn(async move {
            match command.as_str() {
                "telegram_check_session" => {
                    let r = auth::check_session(&session).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::to_value(r).map_err(|e| e.to_string())?)
                }

                "telegram_qr_start" => {
                    let r = auth::qr_login_start(&session).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "svg": r.qr_svg, "expires": r.expires }))
                }

                "telegram_qr_poll" => {
                    match auth::qr_login_poll(&session).await {
                        Ok(QrPollStatus::Waiting) => Ok(serde_json::json!("waiting")),
                        Ok(QrPollStatus::Success { phone }) => Ok(serde_json::json!(format!("success:{}", phone))),
                        Ok(QrPollStatus::PasswordRequired { hint }) => {
                            if hint.is_empty() { Ok(serde_json::json!("password_required")) }
                            else { Ok(serde_json::json!(format!("password_required:{}", hint))) }
                        }
                        Err(e) => Err(e.to_string()),
                    }
                }

                "telegram_send_code" => {
                    let phone: String = serde_json::from_value(args.get("phone").cloned().ok_or("missing 'phone'")?).map_err(|e| e.to_string())?;
                    auth::send_code(&session, &phone).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(null))
                }

                "telegram_verify_code" => {
                    let code: String = serde_json::from_value(args.get("code").cloned().ok_or("missing 'code'")?).map_err(|e| e.to_string())?;
                    match auth::verify_code(&session, &code).await {
                        Ok(phone) => Ok(serde_json::json!(phone)),
                        Err(VerifyError::InvalidCode) => Err("invalid_code".into()),
                        Err(VerifyError::PasswordRequired { hint }) => Err(format!("password_required:{}", hint)),
                        Err(VerifyError::NoSession) => Err("no_session".into()),
                        Err(VerifyError::Other(msg)) => Err(msg),
                    }
                }

                "telegram_verify_2fa" => {
                    let password: String = serde_json::from_value(args.get("password").cloned().ok_or("missing 'password'")?).map_err(|e| e.to_string())?;
                    let r = auth::verify_password(&session, &password).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(r))
                }

                "telegram_logout" => {
                    thumbnail_cache::clear_cache().await;
                    auth::logout(&session).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(null))
                }

                "telegram_takeout_start" => {
                    let id = api::init_takeout(&session).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "takeout_id": id }))
                }

                "telegram_takeout_finish" => {
                    let success: bool = args.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
                    api::finish_takeout(&session, success).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(null))
                }

                "telegram_set_mute" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let mute_until: i64 = serde_json::from_value(args.get("mute_until").or(args.get("muteUntil")).cloned().unwrap_or(serde_json::json!(0))).map_err(|e| e.to_string())?;
                    api::set_mute(&session, chat_id, &chat_type, mute_until).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(null))
                }

                "telegram_toggle_pin" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let pinned: bool = args.get("pinned").and_then(|v| v.as_bool()).unwrap_or(false);
                    api::toggle_pin(&session, chat_id, &chat_type, pinned).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(null))
                }

                "telegram_set_archived" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let archived: bool = args.get("archived").and_then(|v| v.as_bool()).unwrap_or(false);
                    api::set_archived(&session, chat_id, &chat_type, archived).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(null))
                }

                "telegram_reorder_pinned" => {
                    let raw: Vec<serde_json::Value> = serde_json::from_value(args.get("items").cloned().ok_or("missing 'items'")?).map_err(|e| e.to_string())?;
                    let items: Vec<(i64, String)> = raw.into_iter().filter_map(|v| {
                        let cid = v.get("chat_id").or(v.get("chatId")).and_then(|x| x.as_i64())?;
                        let ct = v.get("chat_type").or(v.get("chatType")).and_then(|x| x.as_str())?.to_string();
                        Some((cid, ct))
                    }).collect();
                    api::reorder_pinned(&session, items).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(null))
                }

                "telegram_full_channel_info" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let r = api::full_channel_info(&session, chat_id, &chat_type).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_cancel_op" => {
                    let op_id: String = serde_json::from_value(args.get("op_id").or(args.get("opId")).cloned().ok_or("missing 'op_id'")?).map_err(|e| e.to_string())?;
                    let g = session.lock().await;
                    let map = g.op_cancellation_tokens.clone();
                    drop(g);
                    let cancelled = auth::cancel_op_token(&map, &op_id);
                    if cancelled {
                        tracing::info!("[tg-cancel] op_id={} cancelled", op_id);
                    } else {
                        tracing::debug!("[tg-cancel] op_id={} not found", op_id);
                    }
                    Ok(serde_json::json!({ "cancelled": cancelled }))
                }

                "telegram_list_chats" => {
                    let r = api::list_chats(&session).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_create_folder" => {
                    let name: String = serde_json::from_value(args.get("name").cloned().ok_or("missing 'name'")?).map_err(|e| e.to_string())?;
                    let r = api::create_folder_channel(&session, &name).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_delete_channel" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chatId").or(args.get("chat_id")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    api::delete_channel(&session, chat_id).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_leave_channel" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chatId").or(args.get("chat_id")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chatType").or(args.get("chat_type")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    api::leave_channel(&session, chat_id, &chat_type).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_delete_history" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chatId").or(args.get("chat_id")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chatType").or(args.get("chat_type")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let just_clear: bool = args.get("justClear").or(args.get("just_clear")).and_then(|v| v.as_bool()).unwrap_or(false);
                    let revoke: bool = args.get("revoke").and_then(|v| v.as_bool()).unwrap_or(false);
                    api::delete_history(&session, chat_id, &chat_type, just_clear, revoke).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_list_participants" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chatId").or(args.get("chat_id")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let filter: String = args.get("filter").and_then(|v| v.as_str()).unwrap_or("recent").to_string();
                    let offset: i32 = args.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                    let limit: i32 = args.get("limit").and_then(|v| v.as_i64()).unwrap_or(50) as i32;
                    let search: Option<String> = args.get("search").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let r = api::list_participants(&session, chat_id, &filter, offset, limit, search).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_set_blocked" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chatId").or(args.get("chat_id")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chatType").or(args.get("chat_type")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let blocked: bool = args.get("blocked").and_then(|v| v.as_bool()).unwrap_or(true);
                    api::set_blocked(&session, chat_id, &chat_type, blocked).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_report_peer" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chatId").or(args.get("chat_id")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chatType").or(args.get("chat_type")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let message_ids: Vec<i32> = args.get("messageIds").or(args.get("message_ids")).and_then(|v| v.as_array()).map(|arr| arr.iter().filter_map(|x| x.as_i64().map(|n| n as i32)).collect()).unwrap_or_default();
                    let option: Vec<u8> = args.get("option").and_then(|v| v.as_array()).map(|arr| arr.iter().filter_map(|x| x.as_i64().map(|n| n as u8)).collect()).unwrap_or_default();
                    let message: String = args.get("message").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    api::report_peer(&session, chat_id, &chat_type, message_ids, option, message).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_rename_channel" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chatId").or(args.get("chat_id")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let title: String = serde_json::from_value(args.get("title").cloned().ok_or("missing 'title'")?).map_err(|e| e.to_string())?;
                    api::rename_channel(&session, chat_id, title).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_restore_peer_hashes" => {
                    let raw: Vec<Vec<i64>> = serde_json::from_value(
                        args.get("items").cloned().ok_or("missing 'items'")?,
                    ).map_err(|e| e.to_string())?;
                    let items: Vec<(i64, i64)> = raw
                        .into_iter()
                        .filter_map(|v| if v.len() == 2 { Some((v[0], v[1])) } else { None })
                        .collect();
                    api::restore_peer_hashes(&session, items).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_list_chats_page" => {
                    let offset_date: i32 = args.get("offset_date").or(args.get("offsetDate")).and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                    let offset_id: i32 = args.get("offset_id").or(args.get("offsetId")).and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                    let limit: i32 = args.get("limit").and_then(|v| v.as_i64()).unwrap_or(40) as i32;
                    let offset_peer: Option<(i64, String, i64)> = args
                        .get("offset_peer")
                        .or(args.get("offsetPeer"))
                        .and_then(|v| {
                            let id = v.get("peer_id").or(v.get("peerId"))?.as_i64()?;
                            let t = v.get("peer_type").or(v.get("peerType"))?.as_str()?.to_string();
                            let h = v.get("peer_hash").or(v.get("peerHash"))?.as_i64()?;
                            Some((id, t, h))
                        });
                    let r = api::list_chats_page(&session, offset_date, offset_id, offset_peer, limit)
                        .await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_get_self" => {
                    let r = api::get_self(&session).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_stream_info" => {
                    let slot = stream_server_slot.lock().await;
                    let info = slot.as_ref().ok_or_else(|| "stream server not ready".to_string())?.clone();
                    drop(slot);
                    let info_guard = info.lock().await;
                    Ok(serde_json::json!({
                        "port": info_guard.port,
                        "token": info_guard.token,
                        "base_url": format!("http://127.0.0.1:{}", info_guard.port),
                    }))
                }

                "telegram_delete_messages" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let message_ids: Vec<i32> = serde_json::from_value(args.get("message_ids").or(args.get("messageIds")).cloned().ok_or("missing 'messageIds'")?).map_err(|e| e.to_string())?;
                    let revoke: bool = args.get("revoke").and_then(|v| v.as_bool()).unwrap_or(true);
                    let n = api::delete_messages(&session, chat_id, &chat_type, message_ids, revoke).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "deleted": n }))
                }

                "telegram_forward_messages" => {
                    let from_chat_id: i64 = serde_json::from_value(args.get("from_chat_id").or(args.get("fromChatId")).cloned().ok_or("missing 'fromChatId'")?).map_err(|e| e.to_string())?;
                    let from_chat_type: String = serde_json::from_value(args.get("from_chat_type").or(args.get("fromChatType")).cloned().ok_or("missing 'fromChatType'")?).map_err(|e| e.to_string())?;
                    let to_chat_id: i64 = serde_json::from_value(args.get("to_chat_id").or(args.get("toChatId")).cloned().ok_or("missing 'toChatId'")?).map_err(|e| e.to_string())?;
                    let to_chat_type: String = serde_json::from_value(args.get("to_chat_type").or(args.get("toChatType")).cloned().ok_or("missing 'toChatType'")?).map_err(|e| e.to_string())?;
                    let message_ids: Vec<i32> = serde_json::from_value(args.get("message_ids").or(args.get("messageIds")).cloned().ok_or("missing 'messageIds'")?).map_err(|e| e.to_string())?;
                    let drop_author: bool = args.get("drop_author").or(args.get("dropAuthor")).and_then(|v| v.as_bool()).unwrap_or(false);
                    let drop_captions: bool = args.get("drop_captions").or(args.get("dropCaptions")).and_then(|v| v.as_bool()).unwrap_or(false);
                    api::forward_messages(&session, from_chat_id, &from_chat_type, to_chat_id, &to_chat_type, message_ids, drop_author, drop_captions).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_edit_caption" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let message_id: i32 = serde_json::from_value(args.get("message_id").or(args.get("messageId")).cloned().ok_or("missing 'messageId'")?).map_err(|e| e.to_string())?;
                    let caption: String = serde_json::from_value(args.get("caption").cloned().ok_or("missing 'caption'")?).map_err(|e| e.to_string())?;
                    api::edit_caption(&session, chat_id, &chat_type, message_id, caption).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_search_global" => {
                    let query: String = serde_json::from_value(args.get("query").cloned().ok_or("missing 'query'")?).map_err(|e| e.to_string())?;
                    let limit: u32 = serde_json::from_value(args.get("limit").cloned().unwrap_or(serde_json::json!(50))).map_err(|e| e.to_string())?;
                    let r = api::search_global(&session, &query, limit, fix_ext).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_search_global_hits" => {
                    let query: String = serde_json::from_value(args.get("query").cloned().ok_or("missing 'query'")?).map_err(|e| e.to_string())?;
                    let limit: u32 = serde_json::from_value(args.get("limit").cloned().unwrap_or(serde_json::json!(50))).map_err(|e| e.to_string())?;
                    let r = api::search_global_hits(&session, &query, limit, fix_ext).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_upload_media" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let file_path: String = serde_json::from_value(args.get("file_path").or(args.get("filePath")).cloned().ok_or("missing 'filePath'")?).map_err(|e| e.to_string())?;
                    let caption: Option<String> = args.get("caption").and_then(|v| serde_json::from_value(v.clone()).ok());

                    let path = std::path::PathBuf::from(&file_path);
                    let file_name = path.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| "file".into());

                    let upload_id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                    let host = host.ok_or("Plugin not initialized")?;
                    let host_clone = host.clone();
                    let title = file_name.clone();

                    let _ = host.emit_event(
                        "generic-upload-progress",
                        serde_json::to_value(&GenericUploadProgress {
                            id: upload_id,
                            title: title.clone(),
                            platform: "telegram".into(),
                            percent: 0.0,
                        })
                        .unwrap_or_default(),
                    );

                    tokio::spawn(async move {
                        let result = api::upload_media(&session, chat_id, &chat_type, &path, caption.as_deref()).await;
                        match result {
                            Ok(message_id) => {
                                let _ = host_clone.emit_event(
                                    "generic-upload-progress",
                                    serde_json::to_value(&GenericUploadProgress {
                                        id: upload_id,
                                        title: title.clone(),
                                        platform: "telegram".into(),
                                        percent: 100.0,
                                    })
                                    .unwrap_or_default(),
                                );
                                let _ = host_clone.emit_event(
                                    "generic-upload-complete",
                                    serde_json::to_value(&GenericUploadComplete {
                                        id: upload_id,
                                        title,
                                        platform: "telegram".into(),
                                        success: true,
                                        error: None,
                                        message_id: Some(message_id),
                                    })
                                    .unwrap_or_default(),
                                );
                            }
                            Err(e) => {
                                let _ = host_clone.emit_event(
                                    "generic-upload-complete",
                                    serde_json::to_value(&GenericUploadComplete {
                                        id: upload_id,
                                        title,
                                        platform: "telegram".into(),
                                        success: false,
                                        error: Some(e.to_string()),
                                        message_id: None,
                                    })
                                    .unwrap_or_default(),
                                );
                            }
                        }
                    });

                    Ok(serde_json::json!({ "id": upload_id, "file_name": file_name }))
                }

                "telegram_list_media" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let media_type: Option<String> = args.get("media_type").or(args.get("mediaType")).cloned().and_then(|v| serde_json::from_value(v).ok());
                    let offset: i32 = serde_json::from_value(args.get("offset").cloned().unwrap_or(serde_json::json!(0))).map_err(|e| e.to_string())?;
                    let limit: u32 = serde_json::from_value(args.get("limit").cloned().unwrap_or(serde_json::json!(50))).map_err(|e| e.to_string())?;
                    let r = api::list_media(&session, chat_id, &chat_type, media_type.as_deref(), offset, limit, fix_ext).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_diag_list_media" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let r = api::diag_list_media(&session, chat_id, &chat_type).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_expand_album" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let message_id: i32 = serde_json::from_value(args.get("message_id").or(args.get("messageId")).cloned().ok_or("missing 'messageId'")?).map_err(|e| e.to_string())?;
                    let r = api::expand_album(&session, chat_id, &chat_type, message_id, fix_ext).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_download_media" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let message_id: i32 = serde_json::from_value(args.get("message_id").or(args.get("messageId")).cloned().ok_or("missing 'messageId'")?).map_err(|e| e.to_string())?;
                    let file_name: String = serde_json::from_value(args.get("file_name").or(args.get("fileName")).cloned().ok_or("missing 'fileName'")?).map_err(|e| e.to_string())?;
                    let output_dir: String = serde_json::from_value(args.get("output_dir").or(args.get("outputDir")).cloned().ok_or("missing 'outputDir'")?).map_err(|e| e.to_string())?;
                    let op_id: Option<String> = args.get("op_id").or(args.get("opId")).and_then(|v| v.as_str().map(|s| s.to_string()));

                    let download_id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                    let cancel_token = if let Some(ref oid) = op_id {
                        let g = session.lock().await;
                        let map = g.op_cancellation_tokens.clone();
                        drop(g);
                        let token = auth::register_op_token(&map, oid);
                        tracing::info!("[tg-cancel] registered op_id={} for download_media", oid);
                        token
                    } else {
                        CancellationToken::new()
                    };
                    let op_id_for_cleanup = op_id.clone();
                    let session_for_cleanup = session.clone();
                    {
                        let mut a = active.lock().await;
                        let key = format!("tg:{}:{}", chat_id, message_id);
                        if a.values().any(|(u, _)| u == &key) { return Err("Download already in progress".into()); }
                        a.insert(download_id, (key, cancel_token.clone()));
                    }

                    let host = host.ok_or("Plugin not initialized")?;
                    let active = active.clone();
                    let file_name_clone = file_name.clone();

                    tokio::spawn(async move {
                        let output_path = std::path::PathBuf::from(&output_dir).join(&file_name_clone);
                        let _ = host.emit_event("generic-download-progress", serde_json::to_value(&GenericDownloadProgress { id: download_id, title: file_name_clone.clone(), platform: "telegram".into(), percent: 0.0 }).unwrap_or_default());
                        let queue_id = host_queue::enqueue_external(
                            host.as_ref(),
                            "telegram",
                            &file_name_clone,
                            &output_path,
                            None,
                            Some("telegram_media"),
                            None,
                            None,
                            Some(&file_name_clone),
                        );
                        let (tx, mut rx) = mpsc::channel::<f64>(32);
                        let host_p = host.clone();
                        let fn_p = file_name_clone.clone();
                        let pf = tokio::spawn(async move {
                            while let Some(p) = rx.recv().await {
                                let _ = host_p.emit_event("generic-download-progress", serde_json::to_value(&GenericDownloadProgress { id: download_id, title: fn_p.clone(), platform: "telegram".into(), percent: p }).unwrap_or_default());
                                host_queue::report_progress(host_p.as_ref(), queue_id, p, 0, 0.0).await;
                            }
                        });
                        let result = api::download_media_with_retry(&session, chat_id, &chat_type, message_id, &output_path, tx, &cancel_token).await;
                        let _ = pf.await;
                        active.lock().await.remove(&download_id);
                        if let Some(ref oid) = op_id_for_cleanup {
                            let g = session_for_cleanup.lock().await;
                            let map = g.op_cancellation_tokens.clone();
                            drop(g);
                            auth::unregister_op_token(&map, oid);
                        }
                        match result {
                            Ok(size) => {
                                let pdd = host.plugin_data_dir("telegram");
                                platforms::telegram::bandwidth::record_bytes(&pdd, size);
                                let _ = host.emit_event("generic-download-complete", serde_json::to_value(&GenericDownloadComplete { id: download_id, title: file_name_clone, platform: "telegram".into(), success: true, error: None, file_path: Some(output_path.to_string_lossy().into()), file_size_bytes: Some(size), file_count: Some(1), deleted_count: None }).unwrap_or_default());
                                host_queue::report_complete(host.as_ref(), queue_id, true, Some(&output_path), None, Some(size)).await;
                            }
                            Err(e) => {
                                let err_str = e.to_string();
                                let _ = host.emit_event("generic-download-complete", serde_json::to_value(&GenericDownloadComplete { id: download_id, title: file_name_clone, platform: "telegram".into(), success: false, error: Some(err_str.clone()), file_path: None, file_size_bytes: None, file_count: None, deleted_count: None }).unwrap_or_default());
                                host_queue::report_complete(host.as_ref(), queue_id, false, None, Some(&err_str), None).await;
                            }
                        }
                    });
                    Ok(serde_json::json!({ "id": download_id, "file_name": file_name }))
                }

                "telegram_download_batch" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let chat_title: String = serde_json::from_value(args.get("chat_title").or(args.get("chatTitle")).cloned().ok_or("missing 'chatTitle'")?).map_err(|e| e.to_string())?;
                    let items: Vec<BatchItem> = serde_json::from_value(args.get("items").cloned().ok_or("missing 'items'")?).map_err(|e| e.to_string())?;
                    let output_dir: String = serde_json::from_value(args.get("output_dir").or(args.get("outputDir")).cloned().ok_or("missing 'outputDir'")?).map_err(|e| e.to_string())?;
                    let resume_enabled: bool = args.get("resume").and_then(|v| v.as_bool()).unwrap_or(false);

                    let batch_id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                    let cancel_token = CancellationToken::new();
                    { let mut a = active.lock().await; a.insert(batch_id, (format!("tg-batch:{}", batch_id), cancel_token.clone())); }

                    let host = host.ok_or("Plugin not initialized")?;
                    let active = active.clone();
                    let total_files = items.len() as u32;

                    let plugin_data_dir = host.plugin_data_dir("telegram");
                    let fingerprint = if resume_enabled {
                        let ids: Vec<i32> = items.iter().map(|i| i.message_id).collect();
                        Some(platforms::telegram::resume::compute_fingerprint(chat_id, &ids))
                    } else {
                        None
                    };
                    let resume_completed: std::collections::HashSet<i32> = if let Some(ref fp) = fingerprint {
                        let st = platforms::telegram::resume::load_resume_state(&plugin_data_dir, fp).await;
                        if !st.completed.is_empty() {
                            tracing::info!("[tg-resume] loaded fingerprint={} with {} completed", fp, st.completed.len());
                        }
                        st.completed
                    } else {
                        std::collections::HashSet::new()
                    };

                    let _ = host.emit_event("generic-download-progress", serde_json::to_value(&GenericDownloadProgress { id: batch_id, title: chat_title.clone(), platform: "telegram".into(), percent: 0.0 }).unwrap_or_default());

                    tokio::spawn(async move {
                        let sem = Arc::new(Semaphore::new(concurrent));
                        let completed = Arc::new(std::sync::atomic::AtomicU32::new(0));
                        let failed = Arc::new(std::sync::atomic::AtomicU32::new(0));
                        let skipped = Arc::new(std::sync::atomic::AtomicU32::new(0));
                        let deleted = Arc::new(std::sync::atomic::AtomicU32::new(0));
                        let resumed = Arc::new(std::sync::atomic::AtomicU32::new(0));
                        let resume_completed = Arc::new(resume_completed);
                        let fingerprint = Arc::new(fingerprint);
                        let plugin_data_dir = Arc::new(plugin_data_dir);
                        let mut handles = Vec::new();

                        for item in items {
                            let sem = sem.clone(); let session = session.clone(); let host = host.clone();
                            let chat_type = chat_type.clone(); let chat_title = chat_title.clone();
                            let output_dir = output_dir.clone(); let cancel = cancel_token.clone();
                            let completed = completed.clone(); let failed = failed.clone(); let skipped = skipped.clone(); let deleted = deleted.clone();
                            let resumed = resumed.clone();
                            let resume_completed = resume_completed.clone();
                            let fingerprint = fingerprint.clone();
                            let plugin_data_dir = plugin_data_dir.clone();

                            handles.push(tokio::spawn(async move {
                                if cancel.is_cancelled() { return; }

                                if fingerprint.is_some() && resume_completed.contains(&item.message_id) {
                                    tracing::info!("[tg-resume] skipping {} (already completed)", item.message_id);
                                    resumed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    let done = completed.load(std::sync::atomic::Ordering::Relaxed) + failed.load(std::sync::atomic::Ordering::Relaxed) + skipped.load(std::sync::atomic::Ordering::Relaxed) + deleted.load(std::sync::atomic::Ordering::Relaxed) + resumed.load(std::sync::atomic::Ordering::Relaxed);
                                    let _ = host.emit_event("telegram-batch-file-status", serde_json::to_value(&BatchFileStatus { batch_id, message_id: item.message_id, status: "resumed_skip".into(), percent: 100.0, error: None }).unwrap_or_default());
                                    let _ = host.emit_event("generic-download-progress", serde_json::to_value(&GenericDownloadProgress { id: batch_id, title: chat_title, platform: "telegram".into(), percent: (done as f64 / total_files as f64) * 100.0 }).unwrap_or_default());
                                    return;
                                }

                                let output_path = std::path::PathBuf::from(&output_dir).join(&item.file_name);

                                if output_path.exists() {
                                    if let Ok(meta) = std::fs::metadata(&output_path) {
                                        if meta.len() > 0 {
                                            skipped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            let done = completed.load(std::sync::atomic::Ordering::Relaxed) + failed.load(std::sync::atomic::Ordering::Relaxed) + skipped.load(std::sync::atomic::Ordering::Relaxed) + deleted.load(std::sync::atomic::Ordering::Relaxed);
                                            let _ = host.emit_event("telegram-batch-file-status", serde_json::to_value(&BatchFileStatus { batch_id, message_id: item.message_id, status: "skipped".into(), percent: 100.0, error: None }).unwrap_or_default());
                                            let _ = host.emit_event("generic-download-progress", serde_json::to_value(&GenericDownloadProgress { id: batch_id, title: chat_title, platform: "telegram".into(), percent: (done as f64 / total_files as f64) * 100.0 }).unwrap_or_default());
                                            return;
                                        }
                                    }
                                }

                                let _permit = tokio::select! { p = sem.acquire() => match p { Ok(p) => p, Err(_) => return }, _ = cancel.cancelled() => return };
                                if cancel.is_cancelled() { return; }

                                let item_queue_id = host_queue::enqueue_external(
                                    host.as_ref(),
                                    "telegram",
                                    &item.file_name,
                                    &output_path,
                                    if item.file_size > 0 { Some(item.file_size) } else { None },
                                    Some("telegram_media"),
                                    None,
                                    None,
                                    Some(&item.file_name),
                                );
                                let _ = host.emit_event("telegram-batch-file-status", serde_json::to_value(&BatchFileStatus { batch_id, message_id: item.message_id, status: "downloading".into(), percent: 0.0, error: None }).unwrap_or_default());
                                let (tx, mut rx) = mpsc::channel::<f64>(32);
                                let host_p = host.clone();
                                let mid = item.message_id;
                                let pf = tokio::spawn(async move {
                                    while let Some(p) = rx.recv().await {
                                        let _ = host_p.emit_event("telegram-batch-file-status", serde_json::to_value(&BatchFileStatus { batch_id, message_id: mid, status: "downloading".into(), percent: p, error: None }).unwrap_or_default());
                                        host_queue::report_progress(host_p.as_ref(), item_queue_id, p, 0, 0.0).await;
                                    }
                                });

                                let result = api::download_media_with_retry(&session, chat_id, &chat_type, item.message_id, &output_path, tx, &cancel).await;
                                let _ = pf.await;

                                match result {
                                    Ok(size) => {
                                        completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        platforms::telegram::bandwidth::record_bytes(&plugin_data_dir, size);
                                        let _ = host.emit_event("telegram-batch-file-status", serde_json::to_value(&BatchFileStatus { batch_id, message_id: item.message_id, status: "done".into(), percent: 100.0, error: None }).unwrap_or_default());
                                        host_queue::report_complete(host.as_ref(), item_queue_id, true, Some(&output_path), None, None).await;
                                        if let Some(ref fp) = *fingerprint {
                                            let pdd = plugin_data_dir.clone();
                                            let fp_clone = fp.clone();
                                            let mid = item.message_id;
                                            tokio::spawn(async move {
                                                if let Err(e) = platforms::telegram::resume::mark_completed(&pdd, &fp_clone, mid).await {
                                                    tracing::warn!("[tg-resume] mark_completed failed: {}", e);
                                                }
                                            });
                                        }
                                    }
                                    Err(e) => {
                                        let err_str = e.to_string();
                                        if err_str.contains("cancelled") { return; }
                                        let upper = err_str.to_uppercase();
                                        let is_deleted = upper.contains("MESSAGE_ID_INVALID") || upper.contains("MESSAGEDELETED") || upper.contains("MESSAGE_DELETED");
                                        if is_deleted {
                                            deleted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            tracing::info!("[tg-batch] message {} skipped (deleted/invalid): {}", item.message_id, err_str);
                                            let _ = host.emit_event("telegram-batch-file-status", serde_json::to_value(&BatchFileStatus { batch_id, message_id: item.message_id, status: "skipped_deleted".into(), percent: 100.0, error: Some(err_str.clone()) }).unwrap_or_default());
                                            host_queue::report_complete(host.as_ref(), item_queue_id, false, None, Some(&format!("skipped (deleted): {}", err_str)), None).await;
                                        } else {
                                            failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            let _ = host.emit_event("telegram-batch-file-status", serde_json::to_value(&BatchFileStatus { batch_id, message_id: item.message_id, status: "error".into(), percent: 0.0, error: Some(err_str.clone()) }).unwrap_or_default());
                                            host_queue::report_complete(host.as_ref(), item_queue_id, false, None, Some(&err_str), None).await;
                                        }
                                    }
                                }

                                let done = completed.load(std::sync::atomic::Ordering::Relaxed) + failed.load(std::sync::atomic::Ordering::Relaxed) + skipped.load(std::sync::atomic::Ordering::Relaxed) + deleted.load(std::sync::atomic::Ordering::Relaxed) + resumed.load(std::sync::atomic::Ordering::Relaxed);
                                let _ = host.emit_event("generic-download-progress", serde_json::to_value(&GenericDownloadProgress { id: batch_id, title: chat_title, platform: "telegram".into(), percent: (done as f64 / total_files as f64) * 100.0 }).unwrap_or_default());
                            }));
                        }

                        for h in handles { let _ = h.await; }
                        active.lock().await.remove(&batch_id);
                        let done_count = completed.load(std::sync::atomic::Ordering::Relaxed);
                        let fail_count = failed.load(std::sync::atomic::Ordering::Relaxed);
                        let skip_count = skipped.load(std::sync::atomic::Ordering::Relaxed);
                        let deleted_count = deleted.load(std::sync::atomic::Ordering::Relaxed);
                        let resumed_count = resumed.load(std::sync::atomic::Ordering::Relaxed);
                        let mut error_summary: Vec<String> = Vec::new();
                        if fail_count > 0 { error_summary.push(format!("{} files failed", fail_count)); }
                        if deleted_count > 0 { error_summary.push(format!("{} messages deleted/invalid (skipped)", deleted_count)); }
                        if resumed_count > 0 { tracing::info!("[tg-resume] skipped {} items via fingerprint", resumed_count); }
                        let _ = host.emit_event("generic-download-complete", serde_json::to_value(&GenericDownloadComplete {
                            id: batch_id, title: chat_title, platform: "telegram".into(),
                            success: fail_count == 0 && !cancel_token.is_cancelled(),
                            error: if cancel_token.is_cancelled() { Some("Batch cancelled".into()) } else if !error_summary.is_empty() { Some(error_summary.join("; ")) } else { None },
                            file_path: Some(output_dir), file_size_bytes: None, file_count: Some(done_count + skip_count + resumed_count),
                            deleted_count: if deleted_count > 0 { Some(deleted_count) } else { None },
                        }).unwrap_or_default());
                    });
                    Ok(serde_json::json!(batch_id))
                }

                "telegram_cancel_batch" => {
                    let batch_id: u64 = serde_json::from_value(args.get("batch_id").or(args.get("batchId")).cloned().ok_or("missing 'batchId'")?).map_err(|e| e.to_string())?;
                    let a = active.lock().await;
                    if let Some((key, token)) = a.get(&batch_id) {
                        if key.starts_with("tg-batch:") { token.cancel(); return Ok(serde_json::json!(null)); }
                    }
                    Err("Batch not found".into())
                }

                "telegram_get_thumbnail" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let message_id: i32 = serde_json::from_value(args.get("message_id").or(args.get("messageId")).cloned().ok_or("missing 'messageId'")?).map_err(|e| e.to_string())?;
                    let data = thumbnail_cache::get_thumbnail(&session, chat_id, &chat_type, message_id).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(base64::engine::general_purpose::STANDARD.encode(&data)))
                }

                "telegram_search_media" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let query: String = serde_json::from_value(args.get("query").cloned().ok_or("missing 'query'")?).map_err(|e| e.to_string())?;
                    let media_type: Option<String> = args.get("media_type").or(args.get("mediaType")).cloned().and_then(|v| serde_json::from_value(v).ok());
                    let limit: u32 = serde_json::from_value(args.get("limit").cloned().unwrap_or(serde_json::json!(50))).map_err(|e| e.to_string())?;
                    let r = api::search_media(&session, chat_id, &chat_type, &query, media_type.as_deref(), limit, fix_ext).await.map_err(|e| e.to_string())?;
                    serde_json::to_value(r).map_err(|e| e.to_string())
                }

                "telegram_get_chat_photo" => {
                    let chat_id: i64 = serde_json::from_value(args.get("chat_id").or(args.get("chatId")).cloned().ok_or("missing 'chatId'")?).map_err(|e| e.to_string())?;
                    let chat_type: String = serde_json::from_value(args.get("chat_type").or(args.get("chatType")).cloned().ok_or("missing 'chatType'")?).map_err(|e| e.to_string())?;
                    let data = thumbnail_cache::get_chat_photo(&session, chat_id, &chat_type).await.map_err(|e| e.to_string())?;
                    Ok(serde_json::json!(base64::engine::general_purpose::STANDARD.encode(&data)))
                }

                "telegram_clear_thumbnail_cache" => {
                    thumbnail_cache::clear_cache().await;
                    Ok(serde_json::json!(null))
                }

                "telegram_perf_get" => {
                    let max_threads = platforms::telegram::parallel_download::get_max_threads_cap();
                    let buckets = serde_json::json!([
                        { "min_mb": 0, "max_mb": 1, "threads": platforms::telegram::parallel_download::compute_thread_count(0) },
                        { "min_mb": 1, "max_mb": 5, "threads": platforms::telegram::parallel_download::compute_thread_count(1024 * 1024 * 2) },
                        { "min_mb": 5, "max_mb": 20, "threads": platforms::telegram::parallel_download::compute_thread_count(1024 * 1024 * 10) },
                        { "min_mb": 20, "max_mb": 50, "threads": platforms::telegram::parallel_download::compute_thread_count(1024 * 1024 * 30) },
                        { "min_mb": 50, "max_mb": 0, "threads": platforms::telegram::parallel_download::compute_thread_count(1024 * 1024 * 100) },
                    ]);
                    Ok(serde_json::json!({
                        "max_threads": max_threads,
                        "max_part_size_kb": 1024,
                        "buckets": buckets,
                    }))
                }

                "telegram_clone_start" => {
                    let source_id: i64 = serde_json::from_value(args.get("sourceId").or(args.get("source_id")).cloned().ok_or("missing 'sourceId'")?).map_err(|e| e.to_string())?;
                    let source_type: String = serde_json::from_value(args.get("sourceType").or(args.get("source_type")).cloned().ok_or("missing 'sourceType'")?).map_err(|e| e.to_string())?;
                    let source_title: String = args.get("sourceTitle").or(args.get("source_title")).and_then(|v| v.as_str()).unwrap_or("Origem").to_string();
                    let dest_id_opt: Option<i64> = args.get("destId").or(args.get("dest_id")).and_then(|v| v.as_i64());
                    let dest_type_opt: Option<String> = args.get("destType").or(args.get("dest_type")).and_then(|v| v.as_str().map(|s| s.to_string()));
                    let dest_title_input: Option<String> = args.get("destTitle").or(args.get("dest_title")).and_then(|v| v.as_str().map(|s| s.to_string()));
                    let resume_id: Option<String> = args.get("resumeId").or(args.get("resume_id")).and_then(|v| v.as_str().map(|s| s.to_string()));
                    let opts: platforms::telegram::clone::CloneOptions = match args.get("options").cloned() {
                        Some(v) => serde_json::from_value(v).unwrap_or_default(),
                        None => platforms::telegram::clone::CloneOptions::default(),
                    };

                    let h = host.as_ref().ok_or("Plugin not initialized")?.clone();
                    let pdd = h.plugin_data_dir("telegram");

                    let (dest_id, dest_type, dest_title) = if let Some(id) = dest_id_opt {
                        (id, dest_type_opt.unwrap_or_else(|| "channel".to_string()), dest_title_input.clone().unwrap_or_else(|| format!("Canal {}", id)))
                    } else {
                        let auto_title = dest_title_input.unwrap_or_else(|| format!("{} - clone", source_title));
                        let (id, _hash, title) = platforms::telegram::clone::create_destination_channel(&session, &auto_title)
                            .await
                            .map_err(|e| e.to_string())?;
                        (id, "channel".to_string(), title)
                    };

                    let rt_for_clone = tokio::runtime::Handle::current();
                    let session_id = platforms::telegram::clone::start_clone(
                        session.clone(),
                        h,
                        pdd,
                        rt_for_clone,
                        source_id,
                        source_type,
                        source_title,
                        dest_id,
                        dest_type.clone(),
                        dest_title.clone(),
                        opts,
                        resume_id,
                    ).await.map_err(|e| e.to_string())?;

                    Ok(serde_json::json!({
                        "session_id": session_id,
                        "dest_chat_id": dest_id,
                        "dest_chat_type": dest_type,
                        "dest_title": dest_title,
                    }))
                }

                "telegram_clone_status" => {
                    let id: String = serde_json::from_value(args.get("sessionId").or(args.get("session_id")).cloned().ok_or("missing 'sessionId'")?).map_err(|e| e.to_string())?;
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let s = platforms::telegram::clone::load_session(&pdd, &id).map_err(|e| e.to_string())?;
                    serde_json::to_value(s).map_err(|e| e.to_string())
                }

                "telegram_clone_list" => {
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let list = platforms::telegram::clone::list_sessions(&pdd);
                    serde_json::to_value(list).map_err(|e| e.to_string())
                }

                "telegram_clone_pause" => {
                    let id: String = serde_json::from_value(args.get("sessionId").or(args.get("session_id")).cloned().ok_or("missing 'sessionId'")?).map_err(|e| e.to_string())?;
                    let cancelled = platforms::telegram::clone::cancel_session(&id).await;
                    Ok(serde_json::json!({ "cancelled": cancelled }))
                }

                "telegram_clone_cancel" => {
                    let id: String = serde_json::from_value(args.get("sessionId").or(args.get("session_id")).cloned().ok_or("missing 'sessionId'")?).map_err(|e| e.to_string())?;
                    let _ = platforms::telegram::clone::cancel_session(&id).await;
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let mut s = platforms::telegram::clone::load_session(&pdd, &id).map_err(|e| e.to_string())?;
                    s.status = "cancelled".to_string();
                    platforms::telegram::clone::save_session(&pdd, &s).map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_clone_delete" => {
                    let id: String = serde_json::from_value(args.get("sessionId").or(args.get("session_id")).cloned().ok_or("missing 'sessionId'")?).map_err(|e| e.to_string())?;
                    let _ = platforms::telegram::clone::cancel_session(&id).await;
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    platforms::telegram::clone::delete_session(&pdd, &id).map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_sync_state" => {
                    Ok(serde_json::to_value(platforms::telegram::sync_state::snapshot()).map_err(|e| e.to_string())?)
                }

                "telegram_sync_now" => {
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let updated = platforms::telegram::sync_state::run_once(&session, h.as_ref()).await
                        .map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "updated": updated }))
                }

                "telegram_sync_settings_set" => {
                    if let Some(enabled) = args.get("enabled").and_then(|v| v.as_bool()) {
                        platforms::telegram::sync_state::set_enabled(enabled);
                    }
                    if let Some(n) = args.get("intervalMin").or(args.get("interval_min")).and_then(|v| v.as_u64()) {
                        platforms::telegram::sync_state::set_interval_min(n as u32);
                    }
                    if let Some(h) = host.as_ref() {
                        let mut current = h.get_settings("telegram");
                        if !current.is_object() {
                            current = serde_json::json!({});
                        }
                        if let Some(obj) = current.as_object_mut() {
                            obj.insert("sync_enabled".to_string(), serde_json::json!(platforms::telegram::sync_state::is_enabled()));
                            obj.insert("sync_interval_min".to_string(), serde_json::json!(platforms::telegram::sync_state::interval_min()));
                        }
                        h.save_settings("telegram", current).map_err(|e| e.to_string())?;
                    }
                    Ok(serde_json::to_value(platforms::telegram::sync_state::snapshot()).map_err(|e| e.to_string())?)
                }

                "telegram_accounts_list" => {
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let list = platforms::telegram::accounts::list_profiles(&pdd);
                    serde_json::to_value(list).map_err(|e| e.to_string())
                }

                "telegram_accounts_save_current" => {
                    let label: String = args.get("label").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let phone: Option<String> = args.get("phone").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let user_id: Option<i64> = args.get("userId").or(args.get("user_id")).and_then(|v| v.as_i64());
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let p = platforms::telegram::accounts::save_current_as_profile(
                        &pdd,
                        &label,
                        phone.as_deref(),
                        user_id,
                    ).map_err(|e| e.to_string())?;
                    serde_json::to_value(p).map_err(|e| e.to_string())
                }

                "telegram_accounts_restore" => {
                    let id: String = serde_json::from_value(args.get("id").cloned().ok_or("missing 'id'")?).map_err(|e| e.to_string())?;
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    platforms::telegram::accounts::restore_profile(&pdd, &id).map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true, "needs_restart": true }))
                }

                "telegram_accounts_remove" => {
                    let id: String = serde_json::from_value(args.get("id").cloned().ok_or("missing 'id'")?).map_err(|e| e.to_string())?;
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    platforms::telegram::accounts::remove_profile(&pdd, &id).map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({ "ok": true }))
                }

                "telegram_accounts_rename" => {
                    let id: String = serde_json::from_value(args.get("id").cloned().ok_or("missing 'id'")?).map_err(|e| e.to_string())?;
                    let label: String = serde_json::from_value(args.get("label").cloned().ok_or("missing 'label'")?).map_err(|e| e.to_string())?;
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let p = platforms::telegram::accounts::rename_profile(&pdd, &id, &label).map_err(|e| e.to_string())?;
                    serde_json::to_value(p).map_err(|e| e.to_string())
                }

                "telegram_accounts_backup_now" => {
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let path = platforms::telegram::accounts::backup_current_session(&pdd).map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({
                        "path": path.to_string_lossy(),
                        "name": path.file_name().and_then(|s| s.to_str()).unwrap_or(""),
                    }))
                }

                "telegram_accounts_list_backups" => {
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let list = platforms::telegram::accounts::list_backups(&pdd);
                    let json: Vec<serde_json::Value> = list.into_iter().map(|(name, mtime)| {
                        serde_json::json!({ "name": name, "modified_at": mtime })
                    }).collect();
                    Ok(serde_json::Value::Array(json))
                }

                "telegram_bandwidth_stats" => {
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let s = platforms::telegram::bandwidth::load(&pdd);
                    let pct = if s.quota_bytes > 0 {
                        (s.used_today_bytes as f64 / s.quota_bytes as f64) * 100.0
                    } else {
                        0.0
                    };
                    Ok(serde_json::json!({
                        "used_today_bytes": s.used_today_bytes,
                        "total_used_bytes": s.total_used_bytes,
                        "quota_bytes": s.quota_bytes,
                        "date": s.date,
                        "percentage": pct.min(100.0),
                    }))
                }

                "telegram_bandwidth_set_quota" => {
                    let gb: u64 = serde_json::from_value(
                        args.get("gb").or(args.get("quotaGb")).cloned().ok_or("missing 'gb'")?,
                    ).map_err(|e| e.to_string())?;
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let s = platforms::telegram::bandwidth::set_quota_gb(&pdd, gb).map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({
                        "quota_bytes": s.quota_bytes,
                        "used_today_bytes": s.used_today_bytes,
                    }))
                }

                "telegram_bandwidth_reset" => {
                    let h = host.as_ref().ok_or("Plugin not initialized")?;
                    let pdd = h.plugin_data_dir("telegram");
                    let s = platforms::telegram::bandwidth::reset_today(&pdd).map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({
                        "used_today_bytes": s.used_today_bytes,
                        "date": s.date,
                    }))
                }

                "telegram_perf_set" => {
                    let max_threads: usize = serde_json::from_value(
                        args.get("maxThreads").or(args.get("max_threads")).cloned().ok_or("missing 'maxThreads'")?,
                    ).map_err(|e| e.to_string())?;
                    let clamped = max_threads.clamp(1, 32);
                    platforms::telegram::parallel_download::set_max_threads_cap(clamped);
                    if let Some(h) = host.as_ref() {
                        let mut current = h.get_settings("telegram");
                        if !current.is_object() {
                            current = serde_json::json!({});
                        }
                        if let Some(obj) = current.as_object_mut() {
                            obj.insert("max_threads".to_string(), serde_json::json!(clamped));
                        }
                        h.save_settings("telegram", current).map_err(|e| e.to_string())?;
                    }
                    Ok(serde_json::json!({ "max_threads": clamped }))
                }

                _ => Err(format!("Unknown command: {}", command)),
            }
            }).await.map_err(|e| format!("task join error: {}", e))?
        })
    }

    fn commands(&self) -> Vec<String> {
        vec![
            "telegram_check_session".into(), "telegram_qr_start".into(), "telegram_qr_poll".into(),
            "telegram_send_code".into(), "telegram_verify_code".into(), "telegram_verify_2fa".into(),
            "telegram_logout".into(), "telegram_list_chats".into(), "telegram_list_media".into(),
            "telegram_diag_list_media".into(),
            "telegram_expand_album".into(), "telegram_takeout_start".into(), "telegram_takeout_finish".into(),
            "telegram_cancel_op".into(), "telegram_full_channel_info".into(),
            "telegram_set_mute".into(), "telegram_toggle_pin".into(), "telegram_set_archived".into(), "telegram_reorder_pinned".into(),
            "telegram_download_media".into(), "telegram_download_batch".into(), "telegram_cancel_batch".into(),
            "telegram_get_thumbnail".into(), "telegram_search_media".into(), "telegram_get_chat_photo".into(),
            "telegram_clear_thumbnail_cache".into(), "telegram_get_self".into(),
            "telegram_stream_info".into(), "telegram_list_chats_page".into(),
            "telegram_restore_peer_hashes".into(), "telegram_create_folder".into(),
            "telegram_delete_channel".into(), "telegram_rename_channel".into(),
            "telegram_leave_channel".into(), "telegram_delete_history".into(),
            "telegram_list_participants".into(), "telegram_set_blocked".into(),
            "telegram_report_peer".into(),
            "telegram_perf_get".into(), "telegram_perf_set".into(),
            "telegram_bandwidth_stats".into(), "telegram_bandwidth_set_quota".into(), "telegram_bandwidth_reset".into(),
            "telegram_clone_start".into(), "telegram_clone_status".into(), "telegram_clone_list".into(),
            "telegram_clone_pause".into(), "telegram_clone_cancel".into(), "telegram_clone_delete".into(),
            "telegram_accounts_list".into(), "telegram_accounts_save_current".into(),
            "telegram_accounts_restore".into(), "telegram_accounts_remove".into(),
            "telegram_accounts_rename".into(), "telegram_accounts_backup_now".into(),
            "telegram_accounts_list_backups".into(),
            "telegram_sync_state".into(), "telegram_sync_now".into(), "telegram_sync_settings_set".into(),
            "telegram_delete_messages".into(), "telegram_forward_messages".into(),
            "telegram_edit_caption".into(), "telegram_search_global".into(), "telegram_search_global_hits".into(),
            "telegram_upload_media".into(),
        ]
    }
}

omniget_plugin_sdk::export_plugin!(TelegramPlugin::new());
