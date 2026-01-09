use crate::yt_dlp::{Downloader, MediaProvider, YtDlpClient};
use anyhow::Result;
use dashmap::DashMap;
use rand::{distributions::Alphanumeric, Rng};
use serde::Deserialize;
use std::{
    collections::VecDeque,
    fmt::Display,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use teloxide::{
    dispatching::DpHandlerDescription,
    prelude::*,
    types::{InlineKeyboardButton, InlineKeyboardMarkup, InputFile, MessageId},
};
use tokio::{sync::Mutex, time};

pub const MAX_IN_MEMORY_BYTES: usize = 50 * 1024 * 1024; // 50MB
const PROGRESS_LINES: usize = 6;
pub const PROGRESS_UPDATE_EVERY: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct AppState {
    selections: Arc<DashMap<String, FormatSelection>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            selections: Arc::new(DashMap::new()),
        }
    }
}

#[derive(Clone)]
pub struct AppServices {
    state: AppState,
    media: Arc<dyn MediaProvider>,
    downloader: Arc<dyn Downloader>,
}

impl AppServices {
    pub fn new() -> Self {
        let yt_dlp = Arc::new(YtDlpClient);
        Self {
            state: AppState::default(),
            media: yt_dlp.clone(),
            downloader: yt_dlp,
        }
    }
}

#[derive(Clone)]
struct FormatSelection {
    url: String,
    format_id: String,
    title: Option<String>,
    ext: Option<String>,
}

#[derive(Deserialize)]
pub struct YtDlpInfo {
    pub title: Option<String>,
    pub formats: Vec<YtDlpFormat>,
}

#[derive(Deserialize)]
pub struct YtDlpFormat {
    pub format_id: String,
    pub ext: Option<String>,
    pub format_note: Option<String>,
    pub height: Option<u32>,
    pub width: Option<u32>,
    pub tbr: Option<f64>,
    pub filesize: Option<u64>,
}

#[derive(Clone)]
pub struct ProgressState {
    lines: Arc<Mutex<VecDeque<String>>>,
}

impl ProgressState {
    pub fn new() -> Self {
        Self {
            lines: Arc::new(Mutex::new(VecDeque::with_capacity(PROGRESS_LINES))),
        }
    }

    pub async fn push_line(&self, line: String) {
        let mut lines = self.lines.lock().await;
        if lines.len() == PROGRESS_LINES {
            lines.pop_front();
        }
        lines.push_back(line);
    }

    pub async fn build_text(&self, title: &Option<String>) -> String {
        let mut text = String::new();
        if let Some(title) = title {
            text.push_str(&format!("Downloading: {title}\n"));
        } else {
            text.push_str("Downloading…\n");
        }

        let lines = self.lines.lock().await;
        for line in lines.iter() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if text.len() + trimmed.len() + 1 > 3800 {
                break;
            }
            text.push_str(trimmed);
            text.push('\n');
        }
        text
    }
}

pub enum DownloadResult {
    InMemory { data: Vec<u8>, filename: String },
    TempFile { path: PathBuf, filename: String },
}

#[derive(Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub format_id: String,
    pub progress: ProgressState,
    pub title: Option<String>,
    pub ext: Option<String>,
}

pub fn build_handler() -> Handler<'static, DependencyMap, Result<()>, DpHandlerDescription> {
    dptree::entry()
        .branch(Update::filter_message().filter_map(extract_url).endpoint(handle_url))
        .branch(Update::filter_callback_query().endpoint(handle_callback))
}

fn extract_url(msg: Message) -> Option<(Message, String)> {
    let text = msg.text()?.trim().to_string();
    if text.starts_with("http://") || text.starts_with("https://") {
        Some((msg, text))
    } else {
        None
    }
}

async fn handle_url(bot: Bot, services: AppServices, msg_and_url: (Message, String)) -> Result<()> {
    let (msg, url) = msg_and_url;
    let chat_id = msg.chat.id;
    let placeholder = bot.send_message(chat_id, "Inspecting URL…").await?;

    let info = services.media.fetch_formats(&url).await;
    let info = match info {
        Ok(info) => info,
        Err(err) => {
            report_user_error(&bot, chat_id, placeholder.id, "Failed to inspect URL.", err).await?;
            return Ok(());
        }
    };

    let title = info.title.clone();
    let mut formats = info.formats;
    formats.sort_by(|a, b| {
        let a_height = a.height.unwrap_or(0);
        let b_height = b.height.unwrap_or(0);
        b_height.cmp(&a_height).then_with(|| {
            let a_tbr = (a.tbr.unwrap_or(0.0) * 100.0) as i64;
            let b_tbr = (b.tbr.unwrap_or(0.0) * 100.0) as i64;
            b_tbr.cmp(&a_tbr)
        })
    });

    let mut rows = Vec::new();
    for format in formats.into_iter().take(8) {
        let label = format_label(&format);
        let key = generate_key();
        services.state.selections.insert(
            key.clone(),
            FormatSelection {
                url: url.clone(),
                format_id: format.format_id,
                title: title.clone(),
                ext: format.ext,
            },
        );
        rows.push(vec![InlineKeyboardButton::callback(label, format!("fmt:{key}"))]);
    }

    if rows.is_empty() {
        bot.edit_message_text(chat_id, placeholder.id, "No formats found.")
            .await?;
        return Ok(());
    }

    let keyboard = InlineKeyboardMarkup::new(rows);
    let mut text = "Select a format:".to_string();
    if let Some(title) = title {
        text = format!("Select a format for:\n{title}");
    }

    bot.edit_message_text(chat_id, placeholder.id, text)
        .reply_markup(keyboard)
        .await?;
    Ok(())
}

async fn handle_callback(bot: Bot, services: AppServices, q: CallbackQuery) -> Result<()> {
    let data = q.data.clone().unwrap_or_default();
    let Some(message) = q.message.clone() else {
        return Ok(());
    };
    let chat_id = message.chat.id;

    if !data.starts_with("fmt:") {
        return Ok(());
    }
    let key = data.trim_start_matches("fmt:");
    let selection = match services.state.selections.get(key) {
        Some(sel) => sel.clone(),
        None => {
            bot.edit_message_text(chat_id, message.id, "Selection expired. Try again.")
                .await?;
            return Ok(());
        }
    };

    bot.answer_callback_query(q.id).await?;
    bot.edit_message_text(chat_id, message.id, "Starting download…")
        .await?;

    let progress = ProgressState::new();
    let progress_clone = progress.clone();
    let bot_clone = bot.clone();
    let url = selection.url.clone();
    let format_id = selection.format_id.clone();
    let title = selection.title.clone();
    let progress_title = title.clone();
    let ext = selection.ext.clone();
    let message_id = message.id;

    let (done_tx, mut done_rx) = tokio::sync::watch::channel(false);

    let progress_task = tokio::spawn(async move {
        let mut ticker = time::interval(PROGRESS_UPDATE_EVERY);
        let mut notified = false;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let text = progress_clone.build_text(&progress_title).await;
                    if let Err(err) = bot_clone.edit_message_text(chat_id, message_id, text).await {
                        tracing::warn!("Failed to edit progress message: {err}");
                        if !notified {
                            let _ = bot_clone
                                .send_message(chat_id, format!("Progress update failed.\nError: {err}"))
                                .await;
                            notified = true;
                        }
                    }
                }
                _ = done_rx.changed() => {
                    break;
                }
            }
        }
    });

    let result = services
        .downloader
        .download(DownloadRequest {
            url,
            format_id,
            progress,
            title: title.clone(),
            ext: ext.clone(),
        })
        .await;

    let _ = done_tx.send(true);
    let _ = progress_task.await;

    match result {
        Ok(DownloadResult::InMemory { data, filename }) => {
            if let Err(err) = bot.edit_message_text(chat_id, message_id, "Uploading…").await {
                report_user_error(&bot, chat_id, message_id, "Failed to update status.", err).await?;
            }
            if let Err(err) = bot
                .send_document(chat_id, InputFile::memory(data).file_name(filename))
                .await
            {
                report_user_error(&bot, chat_id, message_id, "Upload failed.", err).await?;
            } else if let Err(err) = bot.edit_message_text(chat_id, message_id, "Done.").await {
                report_user_error(&bot, chat_id, message_id, "Failed to update status.", err).await?;
            }
        }
        Ok(DownloadResult::TempFile { path, filename }) => {
            if let Err(err) = bot.edit_message_text(chat_id, message_id, "Uploading…").await {
                report_user_error(&bot, chat_id, message_id, "Failed to update status.", err).await?;
            }
            if let Err(err) = bot
                .send_document(chat_id, InputFile::file(path.clone()).file_name(filename))
                .await
            {
                report_user_error(&bot, chat_id, message_id, "Upload failed.", err).await?;
            } else if let Err(err) = bot.edit_message_text(chat_id, message_id, "Done.").await {
                report_user_error(&bot, chat_id, message_id, "Failed to update status.", err).await?;
            }
            let _ = tokio::fs::remove_file(path).await;
        }
        Err(err) => {
            report_user_error(&bot, chat_id, message_id, "Download failed.", err).await?;
        }
    }

    Ok(())
}

async fn report_user_error(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    context: &str,
    err: impl Display,
) -> Result<()> {
    let text = format!("{context}\nError: {err}");
    if bot.edit_message_text(chat_id, message_id, text.clone()).await.is_err() {
        let _ = bot.send_message(chat_id, text).await;
    }
    Ok(())
}

fn format_label(format: &YtDlpFormat) -> String {
    let mut parts = Vec::new();
    if let Some(height) = format.height {
        if let Some(width) = format.width {
            parts.push(format!("{width}x{height}"));
        } else {
            parts.push(format!("{height}p"));
        }
    }
    if let Some(note) = &format.format_note {
        parts.push(note.clone());
    }
    if let Some(ext) = &format.ext {
        parts.push(ext.clone());
    }
    if let Some(size) = format.filesize {
        parts.push(human_size(size));
    }
    if parts.is_empty() {
        format.format_id.clone()
    } else {
        format!("{} ({})", format.format_id, parts.join(" · "))
    }
}

fn human_size(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let bytes_f = bytes as f64;
    if bytes_f >= GB {
        format!("{:.2} GB", bytes_f / GB)
    } else if bytes_f >= MB {
        format!("{:.1} MB", bytes_f / MB)
    } else if bytes_f >= KB {
        format!("{:.1} KB", bytes_f / KB)
    } else {
        format!("{} B", bytes)
    }
}

fn generate_key() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

pub fn build_filename(title: Option<String>, ext: Option<String>, fallback_id: &str) -> String {
    let mut name = title.unwrap_or_else(|| fallback_id.to_string());
    name = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == ' ' || c == '_' || c == '-' { c } else { '_' })
        .collect::<String>();
    let ext = ext.unwrap_or_else(|| "bin".to_string());
    format!("{}.{}", name.trim(), ext)
}

pub fn temp_file_path() -> PathBuf {
    let mut rng = rand::thread_rng();
    let suffix: String = (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect();
    std::env::temp_dir().join(format!("yt-dlp-telegram-{suffix}"))
}
