use crate::error::AppError;
use crate::yt_dlp::{Downloader, MediaProvider, YtDlpClient};
use dashmap::DashMap;
use rand::{distributions::Alphanumeric, Rng};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use teloxide::{
    dispatching::DpHandlerDescription,
    prelude::*,
    types::{InlineKeyboardButton, InlineKeyboardMarkup, InputFile, MessageId},
    ApiError, RequestError,
};
use tokio::{sync::Mutex, time};
use tracing::warn;

pub const MAX_IN_MEMORY_BYTES: usize = 50 * 1024 * 1024; // 50MB
const PROGRESS_LINES: usize = 6;
pub const PROGRESS_UPDATE_EVERY: Duration = Duration::from_secs(2);
const UPLOAD_MAX_RETRIES: usize = 3;
const UPLOAD_RETRY_BASE_DELAY: Duration = Duration::from_secs(2);
const UPLOAD_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

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
    pub abr: Option<f64>,
    pub vcodec: Option<String>,
    pub acodec: Option<String>,
    pub filesize: Option<u64>,
    pub filesize_approx: Option<u64>,
    pub duration: Option<f64>,
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

pub fn build_handler() -> Handler<'static, DependencyMap, Result<(), AppError>, DpHandlerDescription> {
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

async fn handle_url(bot: Bot, services: AppServices, msg_and_url: (Message, String)) -> Result<(), AppError> {
    let (msg, url) = msg_and_url;
    let chat_id = msg.chat.id;
    let placeholder = bot.send_message(chat_id, "Inspecting URL…").await?;

    let info = services.media.fetch_formats(&url).await;
    let info = match info {
        Ok(info) => info,
        Err(err) => {
            report_user_error(&bot, chat_id, placeholder.id, "Failed to inspect URL.", err).await?;
            return Ok(())
        }
    };

    let title = info.title.clone();
    let mut formats = info.formats;
    let best_format_ids = best_format_ids(&formats);
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
        let is_best = best_format_ids.contains(&format.format_id);
        let label = format_label(&format, is_best);
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
        return Ok(())
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

async fn handle_callback(bot: Bot, services: AppServices, q: CallbackQuery) -> Result<(), AppError> {
    let data = q.data.clone().unwrap_or_default();
    let Some(message) = q.message.clone() else {
        return Ok(())
    };
    let chat_id = message.chat.id;

    if !data.starts_with("fmt:") {
        return Ok(())
    }
    let key = data.trim_start_matches("fmt:");
    let selection = match services.state.selections.get(key) {
        Some(sel) => sel.clone(),
        None => {
            bot.edit_message_text(chat_id, message.id, "Selection expired. Try again.")
                .await?;
            return Ok(())
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
        let mut last_text: Option<String> = None;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let text = progress_clone.build_text(&progress_title).await;
                    if last_text.as_deref() == Some(text.as_str()) {
                        continue;
                    }
                    match bot_clone.edit_message_text(chat_id, message_id, text.clone()).await {
                        Ok(_) => {
                            last_text = Some(text);
                        }
                        Err(err) => {
                            if is_message_not_modified(&err) {
                                last_text = Some(text);
                                continue;
                            }
                            tracing::warn!("Failed to edit progress message: {err}");
                            if !notified {
                                let _ = bot_clone
                                    .send_message(chat_id, format!("Progress update failed.\nError: {err}"))
                                    .await;
                                notified = true;
                            }
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
            let data = bytes::Bytes::from(data);
            let filename = filename.clone();
            if let Err(err) = upload_with_progress(&bot, chat_id, message_id, move || {
                InputFile::memory(data.clone()).file_name(filename.clone())
            })
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
            let path_for_upload = path.clone();
            let filename_for_upload = filename.clone();
            if let Err(err) = upload_with_progress(&bot, chat_id, message_id, move || {
                InputFile::file(path_for_upload.clone()).file_name(filename_for_upload.clone())
            })
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

async fn upload_with_progress<F>(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    make_file: F,
) -> Result<(), AppError>
where
    F: Fn() -> InputFile,
{
    let (done_tx, mut done_rx) = tokio::sync::watch::channel(false);
    let bot_clone = bot.clone();
    let started_at = time::Instant::now();
    let progress_task = tokio::spawn(async move {
        let mut ticker = time::interval(PROGRESS_UPDATE_EVERY);
        let mut last_text: Option<String> = None;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let elapsed = started_at.elapsed().as_secs();
                    let text = format!("Uploading… {}s elapsed", elapsed);
                    if last_text.as_deref() == Some(text.as_str()) {
                        continue;
                    }
                    match bot_clone.edit_message_text(chat_id, message_id, text.clone()).await {
                        Ok(_) => {
                            last_text = Some(text);
                        }
                        Err(err) => {
                            if is_message_not_modified(&err) {
                                last_text = Some(text);
                                continue;
                            }
                            tracing::warn!("Failed to edit upload progress message: {err}");
                        }
                    }
                }
                _ = done_rx.changed() => {
                    break;
                }
            }
        }
    });

    let result = send_document_with_retry(bot, chat_id, make_file).await;
    let _ = done_tx.send(true);
    let _ = progress_task.await;
    result
}

async fn send_document_with_retry<F>(bot: &Bot, chat_id: ChatId, make_file: F) -> Result<(), AppError>
where
    F: Fn() -> InputFile,
{
    for attempt in 1..=UPLOAD_MAX_RETRIES {
        match bot.send_document(chat_id, make_file()).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                let Some(delay) = retry_delay_for_upload(&err, attempt) else {
                    return Err(err.into());
                };
                if attempt == UPLOAD_MAX_RETRIES {
                    return Err(err.into());
                }
                if delay > Duration::ZERO {
                    warn!("Upload attempt {attempt} failed; retrying in {delay:?}: {err}");
                    time::sleep(delay).await;
                } else {
                    warn!("Upload attempt {attempt} failed; retrying immediately: {err}");
                }
            }
        }
    }
    Ok(())
}

fn retry_delay_for_upload(err: &RequestError, attempt: usize) -> Option<Duration> {
    match err {
        RequestError::RetryAfter(delay) => Some(*delay),
        RequestError::Network(net) if net.is_timeout() || net.is_connect() => {
            let base_ms = UPLOAD_RETRY_BASE_DELAY.as_millis() as u64;
            let factor = 1u64 << (attempt - 1).min(16);
            let mut delay_ms = base_ms.saturating_mul(factor);
            let max_ms = UPLOAD_RETRY_MAX_DELAY.as_millis() as u64;
            if delay_ms > max_ms {
                delay_ms = max_ms;
            }
            let jitter_ms = rand::thread_rng().gen_range(0..=500);
            Some(Duration::from_millis(delay_ms + jitter_ms))
        }
        _ => None,
    }
}

fn is_message_not_modified(err: &RequestError) -> bool {
    matches!(err, RequestError::Api(ApiError::MessageNotModified))
}

async fn report_user_error(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    context: &str,
    err: impl Display,
) -> Result<(), AppError> {
    let text = format!("{context}\nError: {err}");
    if bot.edit_message_text(chat_id, message_id, text.clone()).await.is_err() {
        let _ = bot.send_message(chat_id, text).await;
    }
    Ok(())
}

fn format_label(format: &YtDlpFormat, is_best: bool) -> String {
    let mut parts = Vec::new();
    if let Some(height) = format.height {
        if let Some(width) = format.width {
            parts.push(format!("{width}x{height}"));
        } else {
            parts.push(format!("{height}p"));
        }
    }
    if let Some(kind) = format_kind(format) {
        parts.push(kind.to_string());
    }
    if let Some(note) = &format.format_note {
        parts.push(note.clone());
    }
    if let Some(ext) = &format.ext {
        parts.push(ext.clone());
    }
    if let Some(bitrate) = format_bitrate_kbps(format) {
        parts.push(format!("{bitrate:.0} kbps"));
    }
    if let Some(size) = format.filesize {
        parts.push(human_size(size));
    }
    let label = if parts.is_empty() {
        format.format_id.clone()
    } else {
        format!("{} ({})", format.format_id, parts.join(" · "))
    };
    if is_best {
        format!("⭐ {label}")
    } else {
        label
    }
}

fn format_kind(format: &YtDlpFormat) -> Option<&'static str> {
    let vcodec = format.vcodec.as_deref();
    let acodec = format.acodec.as_deref();
    let v_none = matches!(vcodec, None | Some("none"));
    let a_none = matches!(acodec, None | Some("none"));
    let ext = format.ext.as_deref();

    if is_image_ext(ext) && v_none && a_none {
        return Some("image");
    }
    if v_none && !a_none {
        return Some("audio only");
    }
    if a_none && !v_none {
        return Some("video only");
    }
    if !v_none || !a_none {
        return Some("video+audio");
    }
    None
}

fn best_format_ids(formats: &[YtDlpFormat]) -> HashSet<String> {
    let mut best: HashMap<String, (i64, i64, i64, String)> = HashMap::new();
    for format in formats {
        let ext = format.ext.clone().unwrap_or_else(|| "unknown".to_string());
        let key = format_quality_key(format);
        let entry = best.entry(ext).or_insert_with(|| {
            (key.0, key.1, key.2, format.format_id.clone())
        });
        if (key.0, key.1, key.2) > (entry.0, entry.1, entry.2) {
            *entry = (key.0, key.1, key.2, format.format_id.clone());
        }
    }
    best.into_values().map(|(_, _, _, id)| id).collect()
}

fn format_quality_key(format: &YtDlpFormat) -> (i64, i64, i64) {
    let height = format.height.unwrap_or(0) as i64;
    let bitrate = format_bitrate_kbps(format).unwrap_or(0.0);
    let bitrate_scaled = (bitrate * 100.0) as i64;
    let size = format.filesize.or(format.filesize_approx).unwrap_or(0) as i64;
    (height, bitrate_scaled, size)
}

fn format_bitrate_kbps(format: &YtDlpFormat) -> Option<f64> {
    if let Some(abr) = format.abr {
        if abr > 0.0 {
            return Some(abr);
        }
    }
    if let Some(tbr) = format.tbr {
        if tbr > 0.0 {
            return Some(tbr);
        }
    }
    let duration = format.duration?;
    if duration <= 0.0 {
        return None;
    }
    let bytes = format.filesize.or(format.filesize_approx)?;
    let kbps = (bytes as f64 * 8.0) / duration / 1000.0;
    if kbps > 0.0 {
        Some(kbps)
    } else {
        None
    }
}

fn is_image_ext(ext: Option<&str>) -> bool {
    matches!(
        ext,
        Some("jpg" | "jpeg" | "png" | "webp" | "gif" | "bmp" | "tiff" | "tif" | "svg" | "avif" | "heic" | "heif")
    )
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
