use anyhow::{anyhow, Context, Result};
use bytes::BytesMut;
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
    prelude::*,
    types::{InlineKeyboardButton, InlineKeyboardMarkup, InputFile, MessageId},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::Mutex,
    time,
};

const MAX_IN_MEMORY_BYTES: usize = 50 * 1024 * 1024; // 50MB
const PROGRESS_LINES: usize = 6;
const PROGRESS_UPDATE_EVERY: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct FormatSelection {
    url: String,
    format_id: String,
    title: Option<String>,
    ext: Option<String>,
}

#[derive(Clone, Default)]
struct AppState {
    selections: Arc<DashMap<String, FormatSelection>>,
}

#[derive(Deserialize)]
struct YtDlpInfo {
    title: Option<String>,
    formats: Vec<YtDlpFormat>,
}

#[derive(Deserialize)]
struct YtDlpFormat {
    format_id: String,
    ext: Option<String>,
    format_note: Option<String>,
    height: Option<u32>,
    width: Option<u32>,
    tbr: Option<f64>,
    filesize: Option<u64>,
    vcodec: Option<String>,
    acodec: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    dotenv::dotenv().ok();

    let bot = Bot::from_env();
    let state = AppState::default();

    let handler = dptree::entry()
        .branch(Update::filter_message().filter_map(extract_url).endpoint(handle_url))
        .branch(Update::filter_callback_query().endpoint(handle_callback));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![state])
        .build()
        .dispatch()
        .await;
}

fn extract_url(msg: Message) -> Option<(Message, String)> {
    let text = msg.text()?.trim().to_string();
    if text.starts_with("http://") || text.starts_with("https://") {
        Some((msg, text))
    } else {
        None
    }
}

async fn handle_url(bot: Bot, state: AppState, msg_and_url: (Message, String)) -> Result<()> {
    let (msg, url) = msg_and_url;
    let chat_id = msg.chat.id;
    let placeholder = bot
        .send_message(chat_id, "Inspecting URL…")
        .await?;

    let info = fetch_formats(&url).await;
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
        state.selections.insert(
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

async fn handle_callback(bot: Bot, state: AppState, q: CallbackQuery) -> Result<()> {
    let data = q.data.clone().unwrap_or_default();
    let Some(message) = q.message.clone() else {
        return Ok(());
    };
    let chat_id = message.chat.id;

    if !data.starts_with("fmt:") {
        return Ok(());
    }
    let key = data.trim_start_matches("fmt:");
    let selection = match state.selections.get(key) {
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

    let progress_lines = Arc::new(Mutex::new(VecDeque::with_capacity(PROGRESS_LINES)));
    let progress_lines_clone = progress_lines.clone();
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
                    let text = build_progress_text(&progress_lines_clone, &progress_title).await;
                    if let Err(err) = bot_clone.edit_message_text(chat_id, message_id, text).await {
                        tracing::warn!("Failed to edit progress message: {err}");
                        if !notified {
                            let _ = bot_clone
                                .send_message(
                                    chat_id,
                                    format!("Progress update failed.\nError: {err}"),
                                )
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

    let result = download_with_progress(
        &url,
        &format_id,
        progress_lines,
        title.clone(),
        ext.clone(),
    )
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

async fn fetch_formats(url: &str) -> Result<YtDlpInfo> {
    let output = Command::new("yt-dlp")
        .arg("-J")
        .arg("--no-playlist")
        .arg(url)
        .output()
        .await
        .context("Failed to spawn yt-dlp")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!(stderr.trim().to_string()));
    }

    let info: YtDlpInfo = serde_json::from_slice(&output.stdout)
        .context("Failed to parse yt-dlp JSON output")?;
    Ok(info)
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

enum DownloadResult {
    InMemory { data: Vec<u8>, filename: String },
    TempFile { path: PathBuf, filename: String },
}

async fn download_with_progress(
    url: &str,
    format_id: &str,
    progress_lines: Arc<Mutex<VecDeque<String>>>,
    title: Option<String>,
    ext: Option<String>,
) -> Result<DownloadResult> {
    let mut child = Command::new("yt-dlp")
        .arg("-f")
        .arg(format_id)
        .arg("-o")
        .arg("-")
        .arg("--no-playlist")
        .arg("--newline")
        .arg("--progress")
        .arg(url)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("Failed to spawn yt-dlp")?;

    let stdout = child.stdout.take().context("Missing yt-dlp stdout")?;
    let stderr = child.stderr.take().context("Missing yt-dlp stderr")?;

    let progress_lines_clone = progress_lines.clone();
    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            let mut lines = progress_lines_clone.lock().await;
            if lines.len() == PROGRESS_LINES {
                lines.pop_front();
            }
            lines.push_back(line);
        }
    });

    let mut buffer = BytesMut::new();
    let mut temp_path: Option<PathBuf> = None;
    let mut temp_file: Option<tokio::fs::File> = None;
    let mut reader = BufReader::new(stdout);
    let mut chunk = vec![0u8; 64 * 1024];

    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        if temp_file.is_none() && buffer.len() + read <= MAX_IN_MEMORY_BYTES {
            buffer.extend_from_slice(&chunk[..read]);
        } else {
            if temp_file.is_none() {
                let path = temp_file_path();
                let mut file = tokio::fs::File::create(&path).await?;
                file.write_all(&buffer).await?;
                buffer.clear();
                temp_file = Some(file);
                temp_path = Some(path);
            }
            if let Some(file) = temp_file.as_mut() {
                file.write_all(&chunk[..read]).await?;
            }
        }
    }

    let status = child.wait().await?;
    let _ = stderr_task.await;

    if !status.success() {
        return Err(anyhow!("yt-dlp exited with {status}"));
    }

    let filename = build_filename(title, ext, format_id);
    if let Some(path) = temp_path {
        Ok(DownloadResult::TempFile { path, filename })
    } else {
        Ok(DownloadResult::InMemory {
            data: buffer.to_vec(),
            filename,
        })
    }
}

async fn build_progress_text(
    progress_lines: &Arc<Mutex<VecDeque<String>>>,
    title: &Option<String>,
) -> String {
    let mut text = String::new();
    if let Some(title) = title {
        text.push_str(&format!("Downloading: {title}\n"));
    } else {
        text.push_str("Downloading…\n");
    }

    let lines = progress_lines.lock().await;
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

fn build_filename(title: Option<String>, ext: Option<String>, fallback_id: &str) -> String {
    let mut name = title.unwrap_or_else(|| fallback_id.to_string());
    name = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == ' ' || c == '_' || c == '-' { c } else { '_' })
        .collect::<String>();
    let ext = ext.unwrap_or_else(|| "bin".to_string());
    format!("{}.{}", name.trim(), ext)
}

fn temp_file_path() -> PathBuf {
    let mut rng = rand::thread_rng();
    let suffix: String = (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect();
    std::env::temp_dir().join(format!("yt-dlp-telegram-{suffix}"))
}
