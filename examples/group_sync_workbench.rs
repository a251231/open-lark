use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use clap::{Args, Parser, Subcommand};
use openlark_ai::ai::optical_char_recognition::v1::image::basic_recognize::{
    basic_recognize, BasicRecognizeBody, BasicRecognizeResponse, RecognitionModel,
};
use openlark_communication::im::im::v1::chat::get::GetChatRequest;
use openlark_communication::im::im::v1::image::get::GetImageRequest;
use openlark_communication::im::im::v1::message::create::{
    CreateMessageBody, CreateMessageRequest,
};
use openlark_communication::im::im::v1::message::list::ListMessagesRequest;
use openlark_communication::im::im::v1::message::models::{
    ContainerIdType, ReceiveIdType, SortType,
};
use openlark_communication::im::im::v1::message::resource::get::{
    GetMessageResourceRequest, MessageResourceType,
};
use openlark_core::config::Config;
use openlark_docs::base::bitable::v1::app::table::record::batch_create::{
    BatchCreateRecordRequest, CreateRecordItem,
};
use openlark_docs::base::bitable::v1::app::table::record::CreateRecordRequest;
use openlark_docs::ccm::drive::v1::file::UploadAllRequest;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{watch, Mutex};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    name = "group-sync-workbench",
    version,
    about = "Group message -> OCR -> Bitable sync workbench"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Ui,
    Web(WebArgs),
    ChatInfo(CommonArgs),
    Pull(PullArgs),
    Sync(SyncArgs),
    State(StateArgs),
    ClearState(ClearStateArgs),
    SendText(SendTextArgs),
    SendImage(SendImageArgs),
    ScheduleText(ScheduleTextArgs),
    ScheduleImage(ScheduleImageArgs),
}

#[derive(Debug, Args)]
struct CommonArgs {
    #[arg(long)]
    chat_id: Option<String>,
}

#[derive(Debug, Args)]
struct WebArgs {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 8080)]
    port: u16,
}

#[derive(Debug, Args)]
struct PullArgs {
    #[arg(long)]
    chat_id: Option<String>,
    #[arg(long, default_value_t = 20)]
    page_size: i32,
    #[arg(long, default_value_t = 1)]
    max_pages: u32,
    #[arg(long)]
    start_time: Option<String>,
    #[arg(long)]
    end_time: Option<String>,
}

#[derive(Debug, Args)]
struct SyncArgs {
    #[arg(long)]
    chat_id: Option<String>,
    #[arg(long, default_value_t = 20)]
    page_size: i32,
    #[arg(long, default_value_t = 1)]
    max_pages: u32,
    #[arg(long)]
    start_time: Option<String>,
    #[arg(long)]
    end_time: Option<String>,
    #[arg(long, action = clap::ArgAction::SetTrue)]
    skip_ocr: bool,
    #[arg(long, action = clap::ArgAction::SetTrue)]
    full_sync: bool,
}

#[derive(Debug, Args)]
struct StateArgs {
    #[arg(long)]
    chat_id: Option<String>,
}

#[derive(Debug, Args)]
struct ClearStateArgs {
    #[arg(long)]
    chat_id: Option<String>,
    #[arg(long, action = clap::ArgAction::SetTrue)]
    all: bool,
}

#[derive(Debug, Args)]
struct SendTextArgs {
    #[arg(long)]
    chat_id: Option<String>,
    #[arg(long)]
    text: String,
}

#[derive(Debug, Args)]
struct SendImageArgs {
    #[arg(long)]
    chat_id: Option<String>,
    #[arg(long)]
    image_key: String,
}

#[derive(Debug, Args)]
struct ScheduleTextArgs {
    #[arg(long)]
    chat_id: Option<String>,
    #[arg(long)]
    text: String,
    #[arg(long, default_value_t = 60)]
    interval_secs: u64,
    #[arg(long, default_value_t = 0)]
    max_runs: u32,
}

#[derive(Debug, Args)]
struct ScheduleImageArgs {
    #[arg(long)]
    chat_id: Option<String>,
    #[arg(long)]
    image_key: String,
    #[arg(long, default_value_t = 60)]
    interval_secs: u64,
    #[arg(long, default_value_t = 0)]
    max_runs: u32,
}

#[derive(Debug, Clone)]
struct Runtime {
    config: Config,
    default_chat_id: Option<String>,
    bitable_app_token: Option<String>,
    bitable_table_id: Option<String>,
    ocr_parent_node: Option<String>,
    sync_state_file: String,
}

impl Runtime {
    fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let app_id = require_env("OPENLARK_APP_ID")?;
        let app_secret = require_env("OPENLARK_APP_SECRET")?;
        let base_url = std::env::var("OPENLARK_BASE_URL")
            .unwrap_or_else(|_| "https://open.feishu.cn".to_string());

        let config = Config::builder()
            .app_id(app_id)
            .app_secret(app_secret)
            .base_url(base_url)
            .build();

        Ok(Self {
            config,
            default_chat_id: std::env::var("OPENLARK_DEFAULT_CHAT_ID").ok(),
            bitable_app_token: std::env::var("OPENLARK_BITABLE_APP_TOKEN").ok(),
            bitable_table_id: std::env::var("OPENLARK_BITABLE_TABLE_ID").ok(),
            ocr_parent_node: std::env::var("OPENLARK_OCR_PARENT_NODE").ok(),
            sync_state_file: std::env::var("OPENLARK_SYNC_STATE_FILE")
                .unwrap_or_else(|_| "group_sync_state.json".to_string()),
        })
    }

    fn resolve_chat_id(&self, chat_id: Option<String>) -> Result<String> {
        if let Some(value) = chat_id {
            if !value.trim().is_empty() {
                return Ok(value);
            }
        }

        if let Some(value) = &self.default_chat_id {
            if !value.trim().is_empty() {
                return Ok(value.clone());
            }
        }

        Err(anyhow!(
            "chat_id is required, pass --chat-id or set OPENLARK_DEFAULT_CHAT_ID"
        ))
    }

    fn require_bitable_target(&self) -> Result<(String, String)> {
        let app_token = self
            .bitable_app_token
            .clone()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| anyhow!("OPENLARK_BITABLE_APP_TOKEN is required for sync"))?;

        let table_id = self
            .bitable_table_id
            .clone()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| anyhow!("OPENLARK_BITABLE_TABLE_ID is required for sync"))?;

        Ok((app_token, table_id))
    }

    fn require_ocr_parent_node(&self) -> Result<String> {
        self.ocr_parent_node
            .clone()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| anyhow!("OPENLARK_OCR_PARENT_NODE is required for OCR"))
    }

    fn sync_state_file(&self) -> &str {
        &self.sync_state_file
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SyncState {
    #[serde(default)]
    chats: HashMap<String, ChatSyncState>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ChatSyncState {
    #[serde(default)]
    synced_message_ids: HashSet<String>,
}

#[derive(Debug, Deserialize)]
struct MessageListPage {
    #[serde(default)]
    items: Vec<MessageItem>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    page_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct MessageItem {
    #[serde(default)]
    message_id: String,
    #[serde(default)]
    msg_type: String,
    #[serde(default)]
    create_time: String,
    #[serde(default)]
    sender: MessageSender,
    #[serde(default)]
    body: MessageBody,
}

#[derive(Debug, Default, Deserialize)]
struct MessageSender {
    #[serde(default)]
    id: String,
}

#[derive(Debug, Default, Deserialize)]
struct MessageBody {
    #[serde(default)]
    content: String,
}

#[derive(Debug, Clone, Serialize)]
struct MessageDigest {
    message_id: String,
    msg_type: String,
    create_time: String,
    sender_id: String,
    text: String,
    image_key: String,
    file_key: String,
    ocr_text: String,
}

#[derive(Debug, Clone)]
enum OutboundMessage {
    Text(String),
    Image(String),
}

#[derive(Debug, Clone, Serialize)]
struct SyncSummary {
    success: u32,
    failed: u32,
    skipped_existing: usize,
    full_sync: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let runtime = Runtime::from_env()?;

    match cli.command {
        Command::Ui => run_ui(&runtime).await,
        Command::Web(args) => run_web_workbench(runtime.clone(), args).await,
        Command::ChatInfo(args) => {
            let chat_id = runtime.resolve_chat_id(args.chat_id)?;
            show_chat_info(&runtime, &chat_id).await
        }
        Command::Pull(args) => {
            let chat_id = runtime.resolve_chat_id(args.chat_id)?;
            let messages = pull_message_digests(
                &runtime,
                &chat_id,
                args.page_size,
                args.max_pages,
                args.start_time,
                args.end_time,
            )
            .await?;
            print_message_digests(&messages);
            Ok(())
        }
        Command::Sync(args) => {
            let chat_id = runtime.resolve_chat_id(args.chat_id)?;
            let _ = sync_to_bitable(
                &runtime,
                &chat_id,
                args.page_size,
                args.max_pages,
                args.start_time,
                args.end_time,
                !args.skip_ocr,
                args.full_sync,
            )
            .await?;
            Ok(())
        }
        Command::State(args) => {
            show_sync_state(&runtime, args.chat_id)?;
            Ok(())
        }
        Command::ClearState(args) => {
            clear_sync_state(&runtime, args.chat_id, args.all)?;
            Ok(())
        }
        Command::SendText(args) => {
            let chat_id = runtime.resolve_chat_id(args.chat_id)?;
            send_to_chat(&runtime, &chat_id, &OutboundMessage::Text(args.text)).await
        }
        Command::SendImage(args) => {
            let chat_id = runtime.resolve_chat_id(args.chat_id)?;
            send_to_chat(&runtime, &chat_id, &OutboundMessage::Image(args.image_key)).await
        }
        Command::ScheduleText(args) => {
            let chat_id = runtime.resolve_chat_id(args.chat_id)?;
            schedule_send(
                &runtime,
                &chat_id,
                OutboundMessage::Text(args.text),
                args.interval_secs,
                args.max_runs,
            )
            .await
        }
        Command::ScheduleImage(args) => {
            let chat_id = runtime.resolve_chat_id(args.chat_id)?;
            schedule_send(
                &runtime,
                &chat_id,
                OutboundMessage::Image(args.image_key),
                args.interval_secs,
                args.max_runs,
            )
            .await
        }
    }
}

async fn run_ui(runtime: &Runtime) -> Result<()> {
    loop {
        println!();
        println!("===== Group Sync Workbench =====");
        println!("1) Chat info");
        println!("2) Pull messages");
        println!("3) Sync to Bitable (with OCR)");
        println!("4) Send text message");
        println!("5) Send image message");
        println!("6) Schedule text message");
        println!("7) Schedule image message");
        println!("8) Show sync state");
        println!("9) Clear sync state");
        println!("0) Exit");

        let choice = prompt("Select")?;
        match choice.as_str() {
            "1" => {
                let chat_id = resolve_chat_id_with_prompt(runtime)?;
                show_chat_info(runtime, &chat_id).await?;
            }
            "2" => {
                let chat_id = resolve_chat_id_with_prompt(runtime)?;
                let page_size = prompt_u32("Page size (default: 20)", 20)? as i32;
                let max_pages = prompt_u32("Max pages (default: 1)", 1)?;

                let messages =
                    pull_message_digests(runtime, &chat_id, page_size, max_pages, None, None)
                        .await?;
                print_message_digests(&messages);
            }
            "3" => {
                let chat_id = resolve_chat_id_with_prompt(runtime)?;
                let page_size = prompt_u32("Page size (default: 20)", 20)? as i32;
                let max_pages = prompt_u32("Max pages (default: 1)", 1)?;
                let full_sync = prompt("Full sync? (y/N)")?;
                let is_full_sync = matches!(full_sync.to_lowercase().as_str(), "y" | "yes");
                sync_to_bitable(
                    runtime,
                    &chat_id,
                    page_size,
                    max_pages,
                    None,
                    None,
                    true,
                    is_full_sync,
                )
                .await?;
            }
            "4" => {
                let chat_id = resolve_chat_id_with_prompt(runtime)?;
                let text = prompt("Text")?;
                send_to_chat(runtime, &chat_id, &OutboundMessage::Text(text)).await?;
            }
            "5" => {
                let chat_id = resolve_chat_id_with_prompt(runtime)?;
                let image_key = prompt("Image key")?;
                send_to_chat(runtime, &chat_id, &OutboundMessage::Image(image_key)).await?;
            }
            "6" => {
                let chat_id = resolve_chat_id_with_prompt(runtime)?;
                let text = prompt("Text")?;
                let interval_secs = prompt_u64("Interval seconds (default: 60)", 60)?;
                let max_runs = prompt_u32("Max runs (0 means infinite)", 0)?;
                schedule_send(
                    runtime,
                    &chat_id,
                    OutboundMessage::Text(text),
                    interval_secs,
                    max_runs,
                )
                .await?;
            }
            "7" => {
                let chat_id = resolve_chat_id_with_prompt(runtime)?;
                let image_key = prompt("Image key")?;
                let interval_secs = prompt_u64("Interval seconds (default: 60)", 60)?;
                let max_runs = prompt_u32("Max runs (0 means infinite)", 0)?;
                schedule_send(
                    runtime,
                    &chat_id,
                    OutboundMessage::Image(image_key),
                    interval_secs,
                    max_runs,
                )
                .await?;
            }
            "8" => {
                let chat_id = prompt("Chat id (blank means all chats)")?;
                let chat_id = if chat_id.trim().is_empty() {
                    None
                } else {
                    Some(chat_id)
                };
                show_sync_state(runtime, chat_id)?;
            }
            "9" => {
                let clear_all = prompt("Clear all state? (y/N)")?;
                if matches!(clear_all.to_lowercase().as_str(), "y" | "yes") {
                    clear_sync_state(runtime, None, true)?;
                } else {
                    let chat_id = prompt("Chat id to clear")?;
                    if chat_id.trim().is_empty() {
                        println!("skip clear: chat id empty");
                    } else {
                        clear_sync_state(runtime, Some(chat_id), false)?;
                    }
                }
            }
            "0" => break,
            _ => println!("Unknown option"),
        }
    }

    Ok(())
}

async fn show_chat_info(runtime: &Runtime, chat_id: &str) -> Result<()> {
    let data = GetChatRequest::new(runtime.config.clone())
        .chat_id(chat_id.to_string())
        .execute()
        .await
        .context("get chat info failed")?;

    println!("{}", serde_json::to_string_pretty(&data)?);
    Ok(())
}

async fn pull_message_digests(
    runtime: &Runtime,
    chat_id: &str,
    page_size: i32,
    max_pages: u32,
    start_time: Option<String>,
    end_time: Option<String>,
) -> Result<Vec<MessageDigest>> {
    let pages =
        pull_message_items(runtime, chat_id, page_size, max_pages, start_time, end_time).await?;

    let mut seen = HashSet::new();
    let mut messages = Vec::new();

    for item in pages {
        if item.message_id.is_empty() {
            continue;
        }
        if !seen.insert(item.message_id.clone()) {
            continue;
        }
        messages.push(to_digest(item));
    }

    Ok(messages)
}

async fn pull_message_items(
    runtime: &Runtime,
    chat_id: &str,
    page_size: i32,
    max_pages: u32,
    start_time: Option<String>,
    end_time: Option<String>,
) -> Result<Vec<MessageItem>> {
    let mut page_token: Option<String> = None;
    let mut all_items = Vec::new();
    let total_pages = max_pages.max(1);

    for _ in 0..total_pages {
        let mut request = ListMessagesRequest::new(runtime.config.clone())
            .container_id_type(ContainerIdType::Chat)
            .container_id(chat_id.to_string())
            .sort_type(SortType::ByCreateTimeDesc)
            .page_size(page_size.clamp(1, 50));

        if let Some(value) = &start_time {
            request = request.start_time(value.clone());
        }
        if let Some(value) = &end_time {
            request = request.end_time(value.clone());
        }
        if let Some(token) = &page_token {
            request = request.page_token(token.clone());
        }

        let data = request.execute().await.context("list messages failed")?;
        let page = parse_message_page(data)?;
        all_items.extend(page.items);

        let next_token = page.page_token.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        if !page.has_more || next_token.is_none() {
            break;
        }

        page_token = next_token;
    }

    Ok(all_items)
}

fn parse_message_page(data: Value) -> Result<MessageListPage> {
    serde_json::from_value::<MessageListPage>(data).context("parse message list response failed")
}

fn to_digest(item: MessageItem) -> MessageDigest {
    let content_json = parse_content_json(&item.body.content);

    let text = if item.msg_type == "text" {
        extract_text(&item.body.content, content_json.as_ref())
    } else {
        String::new()
    };

    let image_key = if item.msg_type == "image" {
        extract_image_key(content_json.as_ref())
    } else {
        String::new()
    };

    let file_key = extract_file_key(content_json.as_ref());

    MessageDigest {
        message_id: item.message_id,
        msg_type: item.msg_type,
        create_time: item.create_time,
        sender_id: item.sender.id,
        text,
        image_key,
        file_key,
        ocr_text: String::new(),
    }
}

fn parse_content_json(raw: &str) -> Option<Value> {
    if raw.trim().is_empty() {
        return None;
    }
    serde_json::from_str::<Value>(raw).ok()
}

fn extract_text(raw: &str, value: Option<&Value>) -> String {
    if let Some(text) = value
        .and_then(|v| v.get("text"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
    {
        return text;
    }

    raw.to_string()
}

fn extract_image_key(value: Option<&Value>) -> String {
    value
        .and_then(|v| v.get("image_key"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_default()
}

fn extract_file_key(value: Option<&Value>) -> String {
    value
        .and_then(|v| v.get("file_key"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_default()
}

async fn sync_to_bitable(
    runtime: &Runtime,
    chat_id: &str,
    page_size: i32,
    max_pages: u32,
    start_time: Option<String>,
    end_time: Option<String>,
    with_ocr: bool,
    full_sync: bool,
) -> Result<SyncSummary> {
    let (app_token, table_id) = runtime.require_bitable_target()?;

    let mut messages =
        pull_message_digests(runtime, chat_id, page_size, max_pages, start_time, end_time).await?;

    let mut sync_state = load_sync_state(runtime.sync_state_file())?;
    let known_message_ids = if full_sync {
        HashSet::new()
    } else {
        sync_state
            .chats
            .get(chat_id)
            .map(|chat| chat.synced_message_ids.clone())
            .unwrap_or_default()
    };

    let total_before_filter = messages.len();
    if !full_sync {
        messages.retain(|digest| !known_message_ids.contains(&digest.message_id));
    }
    let skipped_existing = total_before_filter.saturating_sub(messages.len());

    if messages.is_empty() {
        println!(
            "sync finished: no new messages (skipped_existing={})",
            skipped_existing
        );
        return Ok(SyncSummary {
            success: 0,
            failed: 0,
            skipped_existing,
            full_sync,
        });
    }

    if with_ocr {
        for digest in &mut messages {
            if digest.image_key.is_empty() && digest.file_key.is_empty() {
                continue;
            }

            match ocr_message_image(runtime, digest).await {
                Ok(text) => digest.ocr_text = text,
                Err(err) => {
                    digest.ocr_text = format!("OCR_ERROR: {}", err);
                }
            }
        }
    }

    let mut ok = 0_u32;
    let mut failed = 0_u32;

    let records: Vec<(String, Value)> = messages
        .into_iter()
        .map(|digest| {
            let message_id = digest.message_id.clone();
            let fields = json!({
                "message_id": digest.message_id,
                "msg_type": digest.msg_type,
                "create_time": digest.create_time,
                "sender_id": digest.sender_id,
                "text": digest.text,
                "image_key": digest.image_key,
                "file_key": digest.file_key,
                "ocr_text": digest.ocr_text,
            });
            (message_id, fields)
        })
        .collect();

    const BATCH_SIZE: usize = 200;
    for chunk in records.chunks(BATCH_SIZE) {
        let batch_items: Vec<CreateRecordItem> = chunk
            .iter()
            .map(|(_, fields)| CreateRecordItem {
                fields: fields.clone(),
            })
            .collect();

        let batch_result = BatchCreateRecordRequest::new(runtime.config.clone())
            .app_token(app_token.clone())
            .table_id(table_id.clone())
            .client_token(Uuid::new_v4().to_string())
            .records(batch_items)
            .execute()
            .await;

        match batch_result {
            Ok(_) => {
                ok += chunk.len() as u32;
                let chat_state = sync_state.chats.entry(chat_id.to_string()).or_default();
                for (message_id, _) in chunk {
                    chat_state.synced_message_ids.insert(message_id.clone());
                }
            }
            Err(err) => {
                eprintln!(
                    "batch sync failed (size={}): {}, fallback to single writes",
                    chunk.len(),
                    err
                );

                for (message_id, fields) in chunk {
                    let result = CreateRecordRequest::new(runtime.config.clone())
                        .app_token(app_token.clone())
                        .table_id(table_id.clone())
                        .client_token(message_id.clone())
                        .fields(fields.clone())
                        .execute()
                        .await;

                    match result {
                        Ok(_) => {
                            ok += 1;
                            sync_state
                                .chats
                                .entry(chat_id.to_string())
                                .or_default()
                                .synced_message_ids
                                .insert(message_id.clone());
                        }
                        Err(single_err) => {
                            failed += 1;
                            eprintln!("sync failed: {}", single_err);
                        }
                    }
                }
            }
        }
    }

    save_sync_state(runtime.sync_state_file(), &sync_state)?;
    println!(
        "sync finished: success={}, failed={}, skipped_existing={}, full_sync={}",
        ok, failed, skipped_existing, full_sync
    );
    Ok(SyncSummary {
        success: ok,
        failed,
        skipped_existing,
        full_sync,
    })
}

async fn ocr_message_image(runtime: &Runtime, digest: &MessageDigest) -> Result<String> {
    let parent_node = runtime.require_ocr_parent_node()?;
    let image_bytes = download_message_image(runtime, digest).await?;

    let file_stem = if !digest.image_key.is_empty() {
        digest.image_key.clone()
    } else if !digest.file_key.is_empty() {
        digest.file_key.clone()
    } else {
        digest.message_id.clone()
    };

    let file_name = format!("im_{}.bin", file_stem);
    let upload = UploadAllRequest::new(
        runtime.config.clone(),
        file_name,
        parent_node,
        "explorer",
        image_bytes.len(),
        image_bytes,
    )
    .execute()
    .await
    .context("upload image for OCR failed")?;

    let ocr = basic_recognize(
        &runtime.config,
        BasicRecognizeBody {
            file_token: upload.file_token,
            recognition_model: Some(RecognitionModel::Ocr),
        },
    )
    .await
    .context("OCR recognize failed")?;

    Ok(flatten_ocr_text(ocr))
}

async fn download_message_image(runtime: &Runtime, digest: &MessageDigest) -> Result<Vec<u8>> {
    if !digest.image_key.is_empty() {
        return GetImageRequest::new(runtime.config.clone())
            .image_key(digest.image_key.clone())
            .execute()
            .await
            .context("download image by image_key failed");
    }

    if digest.message_id.is_empty() || digest.file_key.is_empty() {
        return Err(anyhow!(
            "missing image_key and fallback identifiers (message_id/file_key)"
        ));
    }

    GetMessageResourceRequest::new(runtime.config.clone())
        .message_id(digest.message_id.clone())
        .file_key(digest.file_key.clone())
        .resource_type(MessageResourceType::Image)
        .execute()
        .await
        .context("download image by message resource failed")
}

fn flatten_ocr_text(resp: BasicRecognizeResponse) -> String {
    resp.data
        .and_then(|data| data.text_blocks)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|block| block.text)
        .collect::<Vec<_>>()
        .join("\n")
}

async fn send_to_chat(runtime: &Runtime, chat_id: &str, payload: &OutboundMessage) -> Result<()> {
    let body = match payload {
        OutboundMessage::Text(text) => CreateMessageBody {
            receive_id: chat_id.to_string(),
            msg_type: "text".to_string(),
            content: json!({ "text": text }).to_string(),
            uuid: Some(Uuid::new_v4().to_string()),
        },
        OutboundMessage::Image(image_key) => CreateMessageBody {
            receive_id: chat_id.to_string(),
            msg_type: "image".to_string(),
            content: json!({ "image_key": image_key }).to_string(),
            uuid: Some(Uuid::new_v4().to_string()),
        },
    };

    CreateMessageRequest::new(runtime.config.clone())
        .receive_id_type(ReceiveIdType::ChatId)
        .execute(body)
        .await
        .context("send message failed")?;

    println!("message sent");
    Ok(())
}

async fn schedule_send(
    runtime: &Runtime,
    chat_id: &str,
    payload: OutboundMessage,
    interval_secs: u64,
    max_runs: u32,
) -> Result<()> {
    let interval_secs = interval_secs.max(1);
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.tick().await;

    println!(
        "scheduler started: interval={}s, max_runs={} (0 means infinite)",
        interval_secs, max_runs
    );
    println!("press Ctrl+C to stop");

    let mut sent = 0_u32;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("scheduler stopped by signal");
                break;
            }
            _ = interval.tick() => {
                let result = send_to_chat(runtime, chat_id, &payload).await;
                match result {
                    Ok(_) => {
                        sent += 1;
                        println!("scheduler sent: {}", sent);
                    }
                    Err(err) => {
                        eprintln!("scheduler send failed: {}", err);
                    }
                }

                if max_runs > 0 && sent >= max_runs {
                    println!("scheduler reached max_runs");
                    break;
                }
            }
        }
    }

    Ok(())
}

fn print_message_digests(messages: &[MessageDigest]) {
    println!("total messages: {}", messages.len());
    for message in messages {
        println!(
            "- id={} type={} sender={} time={} text={} image_key={} file_key={} ocr={}",
            message.message_id,
            message.msg_type,
            message.sender_id,
            message.create_time,
            shorten(&message.text, 40),
            message.image_key,
            message.file_key,
            shorten(&message.ocr_text, 40),
        );
    }
}

fn shorten(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let short: String = iter.by_ref().take(max_chars).collect();
    if iter.next().is_some() {
        format!("{}...", short)
    } else {
        short
    }
}

fn resolve_chat_id_with_prompt(runtime: &Runtime) -> Result<String> {
    let input = prompt("Chat id (blank uses OPENLARK_DEFAULT_CHAT_ID)")?;
    if input.trim().is_empty() {
        runtime.resolve_chat_id(None)
    } else {
        runtime.resolve_chat_id(Some(input))
    }
}

fn prompt(label: &str) -> Result<String> {
    print!("{}: ", label);
    io::stdout().flush()?;

    let mut value = String::new();
    io::stdin().read_line(&mut value)?;
    Ok(value.trim().to_string())
}

fn prompt_u32(label: &str, default: u32) -> Result<u32> {
    let value = prompt(label)?;
    if value.trim().is_empty() {
        return Ok(default);
    }
    value
        .parse::<u32>()
        .with_context(|| format!("invalid u32: {}", value))
}

fn prompt_u64(label: &str, default: u64) -> Result<u64> {
    let value = prompt(label)?;
    if value.trim().is_empty() {
        return Ok(default);
    }
    value
        .parse::<u64>()
        .with_context(|| format!("invalid u64: {}", value))
}

fn require_env(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("missing env: {}", key))
}

fn load_sync_state(path: &str) -> Result<SyncState> {
    let path = Path::new(path);
    if !path.exists() {
        return Ok(SyncState::default());
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("read sync state file failed: {}", path.display()))?;
    if content.trim().is_empty() {
        return Ok(SyncState::default());
    }

    serde_json::from_str::<SyncState>(&content)
        .with_context(|| format!("parse sync state file failed: {}", path.display()))
}

fn save_sync_state(path: &str, state: &SyncState) -> Result<()> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("create sync state directory failed: {}", parent.display())
            })?;
        }
    }

    let content = serde_json::to_string_pretty(state).context("serialize sync state failed")?;
    fs::write(path, content)
        .with_context(|| format!("write sync state file failed: {}", path.display()))?;
    Ok(())
}

fn show_sync_state(runtime: &Runtime, chat_id: Option<String>) -> Result<()> {
    let state = load_sync_state(runtime.sync_state_file())?;

    if let Some(chat_id) = chat_id {
        match state.chats.get(&chat_id) {
            Some(chat_state) => {
                println!(
                    "chat_id={} synced_message_count={}",
                    chat_id,
                    chat_state.synced_message_ids.len()
                );
            }
            None => {
                println!("chat_id={} has no sync state", chat_id);
            }
        }
        return Ok(());
    }

    if state.chats.is_empty() {
        println!("sync state is empty");
        return Ok(());
    }

    println!(
        "sync state file: {} (chat_count={})",
        runtime.sync_state_file(),
        state.chats.len()
    );

    let mut items: Vec<_> = state
        .chats
        .iter()
        .map(|(chat_id, chat_state)| (chat_id.clone(), chat_state.synced_message_ids.len()))
        .collect();
    items.sort_by(|a, b| a.0.cmp(&b.0));

    for (chat_id, count) in items {
        println!("- chat_id={} synced_message_count={}", chat_id, count);
    }

    Ok(())
}

fn clear_sync_state(runtime: &Runtime, chat_id: Option<String>, all: bool) -> Result<()> {
    let mut state = load_sync_state(runtime.sync_state_file())?;

    if all {
        state.chats.clear();
        save_sync_state(runtime.sync_state_file(), &state)?;
        println!("cleared all sync state");
        return Ok(());
    }

    let chat_id = chat_id
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("chat_id is required when --all is not set"))?;

    let removed = state.chats.remove(&chat_id);
    save_sync_state(runtime.sync_state_file(), &state)?;

    if removed.is_some() {
        println!("cleared sync state for chat_id={}", chat_id);
    } else {
        println!("chat_id={} had no sync state", chat_id);
    }

    Ok(())
}

#[derive(Clone)]
struct WebAppState {
    runtime: Runtime,
    scheduler: SchedulerManager,
}

#[derive(Debug, Clone, Serialize)]
struct ScheduledJobSnapshot {
    id: String,
    kind: String,
    chat_id: String,
    payload_preview: String,
    interval_secs: u64,
    max_runs: u32,
    sent: u32,
    status: String,
    created_at: String,
    last_run_at: Option<String>,
    last_error: Option<String>,
}

#[derive(Clone)]
struct ScheduledJobHandle {
    state: Arc<Mutex<ScheduledJobSnapshot>>,
    stop_tx: watch::Sender<bool>,
}

#[derive(Clone, Default)]
struct SchedulerManager {
    jobs: Arc<Mutex<HashMap<String, ScheduledJobHandle>>>,
}

impl SchedulerManager {
    async fn create_job(
        &self,
        runtime: Runtime,
        chat_id: String,
        payload: OutboundMessage,
        interval_secs: u64,
        max_runs: u32,
    ) -> Result<ScheduledJobSnapshot> {
        let interval_secs = interval_secs.max(1);
        let id = Uuid::new_v4().to_string();
        let snapshot = ScheduledJobSnapshot {
            id: id.clone(),
            kind: payload.kind().to_string(),
            chat_id: chat_id.clone(),
            payload_preview: payload.preview(),
            interval_secs,
            max_runs,
            sent: 0,
            status: "running".to_string(),
            created_at: now_rfc3339(),
            last_run_at: None,
            last_error: None,
        };

        let state = Arc::new(Mutex::new(snapshot));
        let (stop_tx, mut stop_rx) = watch::channel(false);

        {
            let mut jobs = self.jobs.lock().await;
            jobs.insert(
                id.clone(),
                ScheduledJobHandle {
                    state: state.clone(),
                    stop_tx,
                },
            );
        }

        let state_for_task = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            interval.tick().await;

            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        if *stop_rx.borrow() {
                            let mut guard = state_for_task.lock().await;
                            guard.status = "stopped".to_string();
                            guard.last_run_at = Some(now_rfc3339());
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        let result = send_to_chat(&runtime, &chat_id, &payload).await;

                        let mut guard = state_for_task.lock().await;
                        guard.last_run_at = Some(now_rfc3339());
                        match result {
                            Ok(_) => {
                                guard.sent += 1;
                                guard.last_error = None;
                                if max_runs > 0 && guard.sent >= max_runs {
                                    guard.status = "finished".to_string();
                                    break;
                                }
                            }
                            Err(err) => {
                                guard.last_error = Some(err.to_string());
                            }
                        }
                    }
                }
            }
        });

        let snapshot = {
            let guard = state.lock().await;
            guard.clone()
        };
        Ok(snapshot)
    }

    async fn list_jobs(&self) -> Vec<ScheduledJobSnapshot> {
        let handles: Vec<ScheduledJobHandle> = {
            let jobs = self.jobs.lock().await;
            jobs.values().cloned().collect()
        };

        let mut items = Vec::with_capacity(handles.len());
        for handle in handles {
            items.push(handle.state.lock().await.clone());
        }
        items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        items
    }

    async fn stop_job(&self, id: &str) -> Result<ScheduledJobSnapshot> {
        let handle = {
            let jobs = self.jobs.lock().await;
            jobs.get(id)
                .cloned()
                .ok_or_else(|| anyhow!("schedule job not found: {}", id))?
        };

        let _ = handle.stop_tx.send(true);

        let mut state = handle.state.lock().await;
        if state.status == "running" {
            state.status = "stopping".to_string();
            state.last_run_at = Some(now_rfc3339());
        }

        Ok(state.clone())
    }
}

impl OutboundMessage {
    fn kind(&self) -> &'static str {
        match self {
            OutboundMessage::Text(_) => "text",
            OutboundMessage::Image(_) => "image",
        }
    }

    fn preview(&self) -> String {
        match self {
            OutboundMessage::Text(text) => shorten(text, 60),
            OutboundMessage::Image(image_key) => format!("image_key={}", image_key),
        }
    }
}

#[derive(Debug, Serialize)]
struct ApiEnvelope {
    ok: bool,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
struct ApiChatIdRequest {
    #[serde(default)]
    chat_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ApiPullRequest {
    #[serde(default)]
    chat_id: Option<String>,
    #[serde(default)]
    page_size: Option<i32>,
    #[serde(default)]
    max_pages: Option<u32>,
    #[serde(default)]
    start_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ApiSyncRequest {
    #[serde(default)]
    chat_id: Option<String>,
    #[serde(default)]
    page_size: Option<i32>,
    #[serde(default)]
    max_pages: Option<u32>,
    #[serde(default)]
    start_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
    #[serde(default)]
    skip_ocr: Option<bool>,
    #[serde(default)]
    full_sync: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct ApiSendTextRequest {
    #[serde(default)]
    chat_id: Option<String>,
    text: String,
}

#[derive(Debug, Deserialize, Default)]
struct ApiSendImageRequest {
    #[serde(default)]
    chat_id: Option<String>,
    image_key: String,
}

#[derive(Debug, Deserialize, Default)]
struct ApiScheduleTextRequest {
    #[serde(default)]
    chat_id: Option<String>,
    text: String,
    #[serde(default)]
    interval_secs: Option<u64>,
    #[serde(default)]
    max_runs: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
struct ApiScheduleImageRequest {
    #[serde(default)]
    chat_id: Option<String>,
    image_key: String,
    #[serde(default)]
    interval_secs: Option<u64>,
    #[serde(default)]
    max_runs: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
struct ApiClearStateRequest {
    #[serde(default)]
    chat_id: Option<String>,
    #[serde(default)]
    all: bool,
}

#[derive(Debug, Serialize)]
struct SyncStateRow {
    chat_id: String,
    synced_message_count: usize,
}

async fn run_web_workbench(runtime: Runtime, args: WebArgs) -> Result<()> {
    let bind_addr = format!("{}:{}", args.host.trim(), args.port);
    let addr: SocketAddr = bind_addr
        .parse()
        .with_context(|| format!("invalid web address: {}", bind_addr))?;

    let app_state = WebAppState {
        runtime,
        scheduler: SchedulerManager::default(),
    };

    let app = Router::new()
        .route("/", get(web_index))
        .route("/api/health", get(api_health))
        .route("/api/chat/info", post(api_chat_info))
        .route("/api/messages/pull", post(api_pull_messages))
        .route("/api/messages/sync", post(api_sync_messages))
        .route("/api/state", post(api_sync_state))
        .route("/api/state/clear", post(api_clear_sync_state))
        .route("/api/send/text", post(api_send_text))
        .route("/api/send/image", post(api_send_image))
        .route("/api/schedule/text", post(api_schedule_text))
        .route("/api/schedule/image", post(api_schedule_image))
        .route("/api/schedules", get(api_schedule_list))
        .route("/api/schedules/:id/stop", post(api_schedule_stop))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind web workbench failed: {}", addr))?;

    println!("web workbench started at http://{}", addr);
    println!("open this URL in browser to operate");
    axum::serve(listener, app)
        .await
        .context("web workbench stopped unexpectedly")
}

async fn web_index() -> Html<&'static str> {
    Html(WEB_UI_HTML)
}

async fn api_health(State(state): State<WebAppState>) -> impl IntoResponse {
    api_ok(
        "ok",
        json!({
            "status": "ok",
            "default_chat_id": state.runtime.default_chat_id.clone(),
            "bitable_configured": state.runtime.bitable_app_token.is_some() && state.runtime.bitable_table_id.is_some(),
            "ocr_configured": state.runtime.ocr_parent_node.is_some(),
            "sync_state_file": state.runtime.sync_state_file(),
        }),
    )
}

async fn api_chat_info(
    State(state): State<WebAppState>,
    Json(req): Json<ApiChatIdRequest>,
) -> impl IntoResponse {
    let result = async {
        let chat_id = state
            .runtime
            .resolve_chat_id(normalize_input(req.chat_id))?;
        let data = GetChatRequest::new(state.runtime.config.clone())
            .chat_id(chat_id)
            .execute()
            .await
            .context("get chat info failed")?;
        Ok::<Value, anyhow::Error>(data)
    }
    .await;

    to_api_response(result, "chat info loaded")
}

async fn api_pull_messages(
    State(state): State<WebAppState>,
    Json(req): Json<ApiPullRequest>,
) -> impl IntoResponse {
    let result = async {
        let chat_id = state
            .runtime
            .resolve_chat_id(normalize_input(req.chat_id))?;
        let messages = pull_message_digests(
            &state.runtime,
            &chat_id,
            req.page_size.unwrap_or(20),
            req.max_pages.unwrap_or(1),
            normalize_input(req.start_time),
            normalize_input(req.end_time),
        )
        .await?;

        Ok::<Value, anyhow::Error>(json!({
            "chat_id": chat_id,
            "count": messages.len(),
            "items": messages,
        }))
    }
    .await;

    to_api_response(result, "messages pulled")
}

async fn api_sync_messages(
    State(state): State<WebAppState>,
    Json(req): Json<ApiSyncRequest>,
) -> impl IntoResponse {
    let result = async {
        let chat_id = state
            .runtime
            .resolve_chat_id(normalize_input(req.chat_id))?;
        let summary = sync_to_bitable(
            &state.runtime,
            &chat_id,
            req.page_size.unwrap_or(20),
            req.max_pages.unwrap_or(1),
            normalize_input(req.start_time),
            normalize_input(req.end_time),
            !req.skip_ocr.unwrap_or(false),
            req.full_sync.unwrap_or(false),
        )
        .await?;

        Ok::<Value, anyhow::Error>(json!({
            "chat_id": chat_id,
            "summary": summary,
        }))
    }
    .await;

    to_api_response(result, "sync completed")
}

async fn api_sync_state(
    State(state): State<WebAppState>,
    Json(req): Json<ApiChatIdRequest>,
) -> impl IntoResponse {
    let result = collect_sync_state_snapshot(&state.runtime, normalize_input(req.chat_id));
    to_api_response(result, "sync state loaded")
}

async fn api_clear_sync_state(
    State(state): State<WebAppState>,
    Json(req): Json<ApiClearStateRequest>,
) -> impl IntoResponse {
    let result = async {
        clear_sync_state(&state.runtime, normalize_input(req.chat_id), req.all)?;
        collect_sync_state_snapshot(&state.runtime, None)
    }
    .await;

    to_api_response(result, "sync state cleared")
}

async fn api_send_text(
    State(state): State<WebAppState>,
    Json(req): Json<ApiSendTextRequest>,
) -> impl IntoResponse {
    let result = async {
        let chat_id = state
            .runtime
            .resolve_chat_id(normalize_input(req.chat_id))?;
        let text = req.text.trim().to_string();
        if text.is_empty() {
            return Err(anyhow!("text is required"));
        }

        send_to_chat(&state.runtime, &chat_id, &OutboundMessage::Text(text)).await?;
        Ok::<Value, anyhow::Error>(json!({ "chat_id": chat_id }))
    }
    .await;

    to_api_response(result, "text message sent")
}

async fn api_send_image(
    State(state): State<WebAppState>,
    Json(req): Json<ApiSendImageRequest>,
) -> impl IntoResponse {
    let result = async {
        let chat_id = state
            .runtime
            .resolve_chat_id(normalize_input(req.chat_id))?;
        let image_key = req.image_key.trim().to_string();
        if image_key.is_empty() {
            return Err(anyhow!("image_key is required"));
        }

        send_to_chat(&state.runtime, &chat_id, &OutboundMessage::Image(image_key)).await?;
        Ok::<Value, anyhow::Error>(json!({ "chat_id": chat_id }))
    }
    .await;

    to_api_response(result, "image message sent")
}

async fn api_schedule_text(
    State(state): State<WebAppState>,
    Json(req): Json<ApiScheduleTextRequest>,
) -> impl IntoResponse {
    let result = async {
        let chat_id = state
            .runtime
            .resolve_chat_id(normalize_input(req.chat_id))?;
        let text = req.text.trim().to_string();
        if text.is_empty() {
            return Err(anyhow!("text is required"));
        }

        let snapshot = state
            .scheduler
            .create_job(
                state.runtime.clone(),
                chat_id,
                OutboundMessage::Text(text),
                req.interval_secs.unwrap_or(60),
                req.max_runs.unwrap_or(0),
            )
            .await?;

        Ok::<Value, anyhow::Error>(json!(snapshot))
    }
    .await;

    to_api_response(result, "text schedule created")
}

async fn api_schedule_image(
    State(state): State<WebAppState>,
    Json(req): Json<ApiScheduleImageRequest>,
) -> impl IntoResponse {
    let result = async {
        let chat_id = state
            .runtime
            .resolve_chat_id(normalize_input(req.chat_id))?;
        let image_key = req.image_key.trim().to_string();
        if image_key.is_empty() {
            return Err(anyhow!("image_key is required"));
        }

        let snapshot = state
            .scheduler
            .create_job(
                state.runtime.clone(),
                chat_id,
                OutboundMessage::Image(image_key),
                req.interval_secs.unwrap_or(60),
                req.max_runs.unwrap_or(0),
            )
            .await?;

        Ok::<Value, anyhow::Error>(json!(snapshot))
    }
    .await;

    to_api_response(result, "image schedule created")
}

async fn api_schedule_list(State(state): State<WebAppState>) -> impl IntoResponse {
    let jobs = state.scheduler.list_jobs().await;
    api_ok("schedule list loaded", json!({ "items": jobs }))
}

async fn api_schedule_stop(
    State(state): State<WebAppState>,
    AxumPath(id): AxumPath<String>,
) -> impl IntoResponse {
    let result = state.scheduler.stop_job(&id).await.map(|v| json!(v));
    to_api_response(result, "schedule stop signal sent")
}

fn to_api_response(result: Result<Value>, ok_message: &str) -> (StatusCode, Json<ApiEnvelope>) {
    match result {
        Ok(data) => api_ok(ok_message, data),
        Err(err) => {
            let status = map_error_status(&err);
            eprintln!("api error: {}", err);
            api_error(status, err.to_string())
        }
    }
}

fn api_ok(message: &str, data: Value) -> (StatusCode, Json<ApiEnvelope>) {
    (
        StatusCode::OK,
        Json(ApiEnvelope {
            ok: true,
            message: message.to_string(),
            data: Some(data),
        }),
    )
}

fn api_error(status: StatusCode, message: String) -> (StatusCode, Json<ApiEnvelope>) {
    (
        status,
        Json(ApiEnvelope {
            ok: false,
            message,
            data: None,
        }),
    )
}

fn map_error_status(err: &anyhow::Error) -> StatusCode {
    let msg = err.to_string().to_lowercase();
    if msg.contains("required")
        || msg.contains("invalid")
        || msg.contains("missing env")
        || msg.contains("chat_id")
    {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn normalize_input(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn collect_sync_state_snapshot(runtime: &Runtime, chat_id: Option<String>) -> Result<Value> {
    let state = load_sync_state(runtime.sync_state_file())?;

    if let Some(chat_id) = chat_id {
        let count = state
            .chats
            .get(&chat_id)
            .map(|v| v.synced_message_ids.len())
            .unwrap_or(0);

        return Ok(json!({
            "sync_state_file": runtime.sync_state_file(),
            "chat_count": state.chats.len(),
            "items": [
                {
                    "chat_id": chat_id,
                    "synced_message_count": count
                }
            ]
        }));
    }

    let mut items: Vec<SyncStateRow> = state
        .chats
        .iter()
        .map(|(chat_id, chat_state)| SyncStateRow {
            chat_id: chat_id.clone(),
            synced_message_count: chat_state.synced_message_ids.len(),
        })
        .collect();
    items.sort_by(|a, b| a.chat_id.cmp(&b.chat_id));

    Ok(json!({
        "sync_state_file": runtime.sync_state_file(),
        "chat_count": items.len(),
        "items": items,
    }))
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

const WEB_UI_HTML: &str = include_str!("group_sync_workbench_web_ui.html");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_text_from_json() {
        let raw = r#"{"text":"hello"}"#;
        let value = parse_content_json(raw);
        assert_eq!(extract_text(raw, value.as_ref()), "hello");
    }

    #[test]
    fn test_extract_text_fallback_raw() {
        let raw = "plain-text";
        let value = parse_content_json(raw);
        assert_eq!(extract_text(raw, value.as_ref()), "plain-text");
    }

    #[test]
    fn test_extract_image_and_file_key() {
        let value = serde_json::json!({
            "image_key": "img_v2_123",
            "file_key": "file_v2_123"
        });

        assert_eq!(extract_image_key(Some(&value)), "img_v2_123");
        assert_eq!(extract_file_key(Some(&value)), "file_v2_123");
    }

    #[test]
    fn test_to_digest_for_image_message() {
        let item = MessageItem {
            message_id: "om_1".to_string(),
            msg_type: "image".to_string(),
            create_time: "1700000000".to_string(),
            sender: MessageSender {
                id: "ou_1".to_string(),
            },
            body: MessageBody {
                content: r#"{"image_key":"img_v2_1","file_key":"file_v2_1"}"#.to_string(),
            },
        };

        let digest = to_digest(item);
        assert_eq!(digest.image_key, "img_v2_1");
        assert_eq!(digest.file_key, "file_v2_1");
        assert!(digest.text.is_empty());
    }

    #[test]
    fn test_load_sync_state_missing_file() {
        let path =
            std::env::temp_dir().join(format!("group_sync_state_missing_{}.json", Uuid::new_v4()));
        let loaded = load_sync_state(path.to_str().expect("valid path")).expect("load should ok");
        assert!(loaded.chats.is_empty());
    }

    #[test]
    fn test_sync_state_round_trip() {
        let path = std::env::temp_dir().join(format!(
            "group_sync_state_roundtrip_{}.json",
            Uuid::new_v4()
        ));
        let path_str = path.to_str().expect("valid path");

        let mut state = SyncState::default();
        let chat_state = state.chats.entry("oc_test".to_string()).or_default();
        chat_state
            .synced_message_ids
            .insert("om_test_001".to_string());

        save_sync_state(path_str, &state).expect("save should ok");
        let loaded = load_sync_state(path_str).expect("load should ok");

        assert!(loaded
            .chats
            .get("oc_test")
            .expect("chat state should exist")
            .synced_message_ids
            .contains("om_test_001"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_normalize_input() {
        assert_eq!(normalize_input(None), None);
        assert_eq!(normalize_input(Some("   ".to_string())), None);
        assert_eq!(
            normalize_input(Some("  oc_test  ".to_string())),
            Some("oc_test".to_string())
        );
    }

    #[test]
    fn test_map_error_status_bad_request() {
        let err = anyhow!("chat_id is required");
        let status = map_error_status(&err);
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    }
}
