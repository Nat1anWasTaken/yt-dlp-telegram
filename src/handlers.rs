use crate::error::AppError;
use crate::tasks::{
    DownloadTask, FormatMeta, FormatSelection, MediaKind, TargetFormat, TaskId, TaskOutcome,
    TaskRegistry, TaskState, build_cancel_callback, build_target_callback, build_task_callback,
    parse_cancel_callback, parse_target_callback, parse_task_callback,
};
use crate::yt_dlp::{Downloader, MediaProvider, YtDlpClient};
use rand::{Rng, distributions::Alphanumeric};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    fmt::Display,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use teloxide::{
    ApiError, RequestError,
    dispatching::DpHandlerDescription,
    prelude::*,
    types::{InlineKeyboardButton, InlineKeyboardMarkup, InputFile, MessageId},
};
use tokio::{io::AsyncReadExt, process::Command, sync::Mutex, time};
use tracing::{Instrument, debug, error, info, instrument, trace, warn};

pub const MAX_IN_MEMORY_BYTES: usize = 50 * 1024 * 1024; // 50MB
const PROGRESS_LINES: usize = 6;
pub const PROGRESS_UPDATE_EVERY: Duration = Duration::from_secs(2);
const SELECTION_TIMEOUT: Duration = Duration::from_secs(30);
const UPLOAD_MAX_RETRIES: usize = 3;
const UPLOAD_RETRY_BASE_DELAY: Duration = Duration::from_secs(2);
const UPLOAD_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

type TargetSelectionResult = Result<
    (
        String,
        Option<String>,
        Option<String>,
        FormatSelection,
        TargetFormat,
        Arc<AtomicBool>,
    ),
    &'static str,
>;

#[derive(Clone)]
struct AppState {
    tasks: TaskRegistry,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            tasks: TaskRegistry::new(),
        }
    }
}

#[derive(Clone)]
pub struct AppServices {
    state: AppState,
    media: Arc<dyn MediaProvider>,
    downloader: Arc<dyn Downloader>,
    allowed_user_ids: Option<HashSet<u64>>,
}

impl AppServices {
    pub fn new() -> Self {
        let yt_dlp = Arc::new(YtDlpClient);
        info!(event = "services_init");
        Self {
            state: AppState::default(),
            media: yt_dlp.clone(),
            downloader: yt_dlp,
            allowed_user_ids: read_allowed_user_ids(),
        }
    }
}

fn read_allowed_user_ids() -> Option<HashSet<u64>> {
    let raw = env::var("ALLOWED_USER_IDS").ok()?;
    let mut ids = HashSet::new();
    for entry in raw.split(',') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            debug!(event = "allowed_user_id_empty_entry");
            continue;
        }
        match trimmed.parse::<u64>() {
            Ok(id) => {
                debug!(event = "allowed_user_id_added", user_id = id);
                ids.insert(id);
            }
            Err(err) => {
                warn!(
                    event = "allowed_user_id_invalid",
                    entry = trimmed,
                    error = %err
                );
            }
        }
    }
    if ids.is_empty() {
        info!(event = "allowed_user_ids_empty");
        None
    } else {
        info!(event = "allowed_user_ids_loaded", count = ids.len());
        Some(ids)
    }
}

fn is_user_allowed(allowed_user_ids: &Option<HashSet<u64>>, user_id: u64) -> bool {
    let allowed = allowed_user_ids
        .as_ref()
        .map(|allowed| allowed.contains(&user_id))
        .unwrap_or(true);
    trace!(event = "user_allowed_check", user_id, allowed);
    allowed
}

#[instrument(skip(bot))]
async fn reject_user(bot: &Bot, chat_id: ChatId, user_id: u64) -> Result<(), AppError> {
    info!(event = "reject_user", chat_id = %chat_id, user_id);
    bot.send_message(
        chat_id,
        format!("You do not have permission to use this bot. Your user ID is: {user_id}"),
    )
    .await?;
    Ok(())
}

#[derive(Deserialize)]
pub struct YtDlpInfo {
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub channel: Option<String>,
    pub creator: Option<String>,
    pub artist: Option<String>,
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

    #[instrument(skip(self))]
    pub async fn push_line(&self, line: String) {
        let mut lines = self.lines.lock().await;
        if lines.len() == PROGRESS_LINES {
            trace!(event = "progress_line_drop");
            lines.pop_front();
        }
        lines.push_back(line);
    }

    #[instrument(skip(self, title))]
    pub async fn build_text(&self, phase: &str, title: &Option<String>) -> String {
        let mut text = String::new();
        if let Some(title) = title {
            text.push_str(&format!("{phase}: {title}\n"));
        } else {
            text.push_str(&format!("{phase}…\n"));
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
    InMemory { data: Vec<u8> },
    TempFile { path: PathBuf },
}

#[derive(Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub format_id: String,
    pub progress: ProgressState,
    pub cancel: Arc<AtomicBool>,
}

struct RunDownloadContext {
    task_id: TaskId,
    chat_id: ChatId,
    message_id: MessageId,
    url: String,
    title: Option<String>,
    author: Option<String>,
    selection: FormatSelection,
    target: TargetFormat,
    cancel: Arc<AtomicBool>,
}

pub fn build_handler() -> Handler<'static, DependencyMap, Result<(), AppError>, DpHandlerDescription>
{
    dptree::entry()
        .branch(
            Update::filter_message()
                .filter_map(extract_url)
                .endpoint(handle_url),
        )
        .branch(
            Update::filter_message()
                .filter_map(extract_non_url)
                .endpoint(handle_non_url),
        )
        .branch(Update::filter_callback_query().endpoint(handle_callback))
}

fn extract_url(msg: Message) -> Option<(Message, String)> {
    let text = msg.text()?.trim().to_string();
    if text.starts_with("http://") || text.starts_with("https://") {
        debug!(event = "extract_url_match");
        Some((msg, text))
    } else {
        trace!(event = "extract_url_miss");
        None
    }
}

fn extract_non_url(msg: Message) -> Option<Message> {
    let text = msg.text()?.trim();
    if text.starts_with("http://") || text.starts_with("https://") {
        trace!(event = "extract_non_url_miss");
        None
    } else {
        debug!(event = "extract_non_url_match");
        Some(msg)
    }
}

fn message_user_id(msg: &Message) -> u64 {
    msg.from()
        .map(|user| user.id.0)
        .unwrap_or_else(|| u64::try_from(msg.chat.id.0).unwrap_or_default())
}

#[instrument(skip(bot, services, msg_and_url))]
async fn handle_url(
    bot: Bot,
    services: AppServices,
    msg_and_url: (Message, String),
) -> Result<(), AppError> {
    let (msg, url) = msg_and_url;
    let chat_id = msg.chat.id;
    let user_id = message_user_id(&msg);
    info!(
        event = "handle_url_start",
        chat_id = %chat_id,
        user_id,
        url = %url
    );
    if !is_user_allowed(&services.allowed_user_ids, user_id) {
        warn!(event = "handle_url_rejected", chat_id = %chat_id, user_id);
        reject_user(&bot, chat_id, user_id).await?;
        return Ok(());
    }
    let placeholder = bot.send_message(chat_id, "Inspecting URL…").await?;

    let info = services.media.fetch_formats(&url).await;
    let info = match info {
        Ok(info) => info,
        Err(err) => {
            error!(
                event = "fetch_formats_failed",
                chat_id = %chat_id,
                user_id,
                error = %err
            );
            report_user_error(&bot, chat_id, placeholder.id, "Failed to inspect URL.", err).await?;
            return Ok(());
        }
    };

    let title = info.title.clone();
    let author = select_author(&info);
    let mut formats = info.formats;
    let best_format_ids = best_format_ids(&formats);
    debug!(
        event = "formats_loaded",
        chat_id = %chat_id,
        user_id,
        format_count = formats.len()
    );
    formats.sort_by(|a, b| {
        let a_height = a.height.unwrap_or(0);
        let b_height = b.height.unwrap_or(0);
        b_height.cmp(&a_height).then_with(|| {
            let a_tbr = (a.tbr.unwrap_or(0.0) * 100.0) as i64;
            let b_tbr = (b.tbr.unwrap_or(0.0) * 100.0) as i64;
            b_tbr.cmp(&a_tbr)
        })
    });

    let mut formats_map = HashMap::new();
    for format in formats.iter().take(8) {
        let kind = media_kind(format);
        formats_map.insert(
            format.format_id.clone(),
            FormatMeta {
                ext: format.ext.clone(),
                kind,
            },
        );
    }
    let task = DownloadTask::new(
        chat_id,
        placeholder.id,
        url.clone(),
        title.clone(),
        author.clone(),
        formats_map,
    );
    let task_id = task.id.clone();
    services.state.tasks.insert(task);
    info!(
        event = "task_created",
        chat_id = %chat_id,
        task_id = %task_id
    );

    let mut rows = Vec::new();
    for format in formats.into_iter().take(8) {
        let is_best = best_format_ids.contains(&format.format_id);
        let label = format_label(&format, is_best);
        let callback = build_task_callback(&task_id, &format.format_id);
        rows.push(vec![InlineKeyboardButton::callback(label, callback)]);
    }

    if rows.is_empty() {
        warn!(event = "formats_empty", chat_id = %chat_id, user_id);
        bot.edit_message_text(chat_id, placeholder.id, "No formats found.")
            .await?;
        return Ok(());
    }

    rows.push(vec![InlineKeyboardButton::callback(
        "Cancel",
        build_cancel_callback(&task_id),
    )]);
    let keyboard = InlineKeyboardMarkup::new(rows);
    let mut text = "Select a format:".to_string();
    if let Some(title) = title {
        text = format!("Select a format for:\n{title}");
    }

    bot.edit_message_text(chat_id, placeholder.id, text)
        .reply_markup(keyboard)
        .await?;

    spawn_waiting_format_timeout(
        bot.clone(),
        services.state.tasks.clone(),
        task_id,
        chat_id,
        placeholder.id,
    );
    Ok(())
}

#[instrument(skip(bot, services, msg))]
async fn handle_non_url(bot: Bot, services: AppServices, msg: Message) -> Result<(), AppError> {
    let chat_id = msg.chat.id;
    let user_id = message_user_id(&msg);
    info!(event = "handle_non_url", chat_id = %chat_id, user_id);
    if !is_user_allowed(&services.allowed_user_ids, user_id) {
        warn!(event = "handle_non_url_rejected", chat_id = %chat_id, user_id);
        reject_user(&bot, chat_id, user_id).await?;
        return Ok(());
    }
    bot.send_message(chat_id, "Send any URL.").await?;
    Ok(())
}

#[instrument(skip(bot, services, q))]
async fn handle_callback(
    bot: Bot,
    services: AppServices,
    q: CallbackQuery,
) -> Result<(), AppError> {
    let data = q.data.clone().unwrap_or_default();
    let Some(message) = q.message.clone() else {
        warn!(event = "callback_missing_message");
        return Ok(());
    };
    let chat_id = message.chat.id;
    let user_id = q.from.id.0;
    info!(
        event = "handle_callback_start",
        chat_id = %chat_id,
        user_id,
        callback_data = %data
    );
    if !is_user_allowed(&services.allowed_user_ids, user_id) {
        warn!(event = "handle_callback_rejected", chat_id = %chat_id, user_id);
        bot.answer_callback_query(q.id).await?;
        reject_user(&bot, chat_id, user_id).await?;
        return Ok(());
    }

    if let Some(task_id) = parse_cancel_callback(&data) {
        info!(event = "callback_cancel", task_id = %task_id);
        handle_cancel_task(&bot, services, q, message.id, chat_id, task_id).await
    } else if let Some((task_id, format_id)) = parse_task_callback(&data) {
        info!(event = "callback_format_select", task_id = %task_id, format_id = %format_id);
        handle_format_selection(&bot, services, q, message.id, chat_id, task_id, format_id).await
    } else if let Some((task_id, target_ext)) = parse_target_callback(&data) {
        info!(event = "callback_target_select", task_id = %task_id, target_ext = %target_ext);
        handle_target_selection(&bot, services, q, message.id, chat_id, task_id, target_ext).await
    } else {
        warn!(event = "callback_unhandled", callback_data = %data);
        Ok(())
    }
}

#[instrument(skip(bot, services, q))]
async fn handle_format_selection(
    bot: &Bot,
    services: AppServices,
    q: CallbackQuery,
    message_id: MessageId,
    chat_id: ChatId,
    task_id: TaskId,
    format_id: String,
) -> Result<(), AppError> {
    let Some(task) = services.state.tasks.get(&task_id) else {
        warn!(event = "task_missing", task_id = %task_id);
        bot.edit_message_text(chat_id, message_id, "Task expired. Try again.")
            .await?;
        return Ok(());
    };

    let selection_result: Result<
        (Option<String>, FormatSelection, Vec<TargetFormat>),
        &'static str,
    > = {
        let mut task = task.lock().await;
        if task.message_id != message_id || task.chat_id != chat_id {
            warn!(event = "task_message_mismatch", task_id = %task_id);
            Err("Task mismatch. Try again.")
        } else {
            match &task.state {
                TaskState::WaitingFormat { formats } => {
                    if let Some(meta) = formats.get(&format_id).cloned() {
                        let selection = FormatSelection {
                            format_id: format_id.clone(),
                            ext: meta.ext,
                            kind: meta.kind,
                        };
                        let targets = target_formats_for(selection.kind, selection.ext.as_deref());
                        task.state = TaskState::SelectingTarget {
                            selection: selection.clone(),
                            targets: targets.clone(),
                        };
                        info!(
                            event = "format_selected",
                            task_id = %task_id,
                            format_id = %format_id
                        );
                        Ok((task.title.clone(), selection, targets))
                    } else {
                        warn!(
                            event = "format_missing",
                            task_id = %task_id,
                            format_id = %format_id
                        );
                        Err("Format expired. Try again.")
                    }
                }
                _ => {
                    warn!(event = "task_already_started", task_id = %task_id);
                    Err("Task already started.")
                }
            }
        }
    };

    let (title, selection, targets) = match selection_result {
        Ok(data) => data,
        Err(error_message) => {
            warn!(event = "format_selection_error", task_id = %task_id, error = error_message);
            bot.edit_message_text(chat_id, message_id, error_message)
                .await?;
            return Ok(());
        }
    };

    bot.answer_callback_query(q.id).await?;

    if targets.is_empty() {
        warn!(event = "target_list_empty", task_id = %task_id);
        bot.edit_message_text(chat_id, message_id, "No output formats found.")
            .await?;
        return Ok(());
    }

    let mut rows = targets
        .iter()
        .map(|target| {
            vec![InlineKeyboardButton::callback(
                target_label(&selection.kind, &target.ext),
                build_target_callback(&task_id, &target.ext),
            )]
        })
        .collect::<Vec<_>>();
    rows.push(vec![InlineKeyboardButton::callback(
        "Cancel",
        build_cancel_callback(&task_id),
    )]);
    let keyboard = InlineKeyboardMarkup::new(rows);

    let mut text = "Select an output format:".to_string();
    if let Some(title) = title {
        text = format!("Select an output format for:\n{title}");
    }

    bot.edit_message_text(chat_id, message_id, text)
        .reply_markup(keyboard)
        .await?;

    spawn_target_selection_timeout(
        bot.clone(),
        services.state.tasks.clone(),
        task_id,
        chat_id,
        message_id,
    );
    Ok(())
}

#[instrument(skip(bot, services, q))]
async fn handle_target_selection(
    bot: &Bot,
    services: AppServices,
    q: CallbackQuery,
    message_id: MessageId,
    chat_id: ChatId,
    task_id: TaskId,
    target_ext: String,
) -> Result<(), AppError> {
    let Some(task) = services.state.tasks.get(&task_id) else {
        warn!(event = "task_missing", task_id = %task_id);
        bot.edit_message_text(chat_id, message_id, "Task expired. Try again.")
            .await?;
        return Ok(());
    };

    let selection_result: TargetSelectionResult = {
        let mut task = task.lock().await;
        if task.message_id != message_id || task.chat_id != chat_id {
            warn!(event = "task_message_mismatch", task_id = %task_id);
            Err("Task mismatch. Try again.")
        } else if let TaskState::SelectingTarget { selection, targets } = &task.state {
            let selection = selection.clone();
            let targets = targets.clone();
            if targets.iter().any(|target| target.ext == target_ext) {
                let target = TargetFormat {
                    ext: target_ext.clone(),
                };
                task.state = TaskState::Downloading {
                    selection: selection.clone(),
                    target: target.clone(),
                };
                Ok((
                    task.url.clone(),
                    task.title.clone(),
                    task.author.clone(),
                    selection,
                    target,
                    task.cancel.clone(),
                ))
            } else {
                warn!(
                    event = "target_missing",
                    task_id = %task_id,
                    target_ext = %target_ext
                );
                Err("Format expired. Try again.")
            }
        } else {
            warn!(event = "task_already_started", task_id = %task_id);
            Err("Task already started.")
        }
    };

    let (url, title, author, selection, target, cancel) = match selection_result {
        Ok(data) => data,
        Err(error_message) => {
            warn!(event = "target_selection_error", task_id = %task_id, error = error_message);
            bot.edit_message_text(chat_id, message_id, error_message)
                .await?;
            return Ok(());
        }
    };

    bot.answer_callback_query(q.id).await?;
    bot.edit_message_text(chat_id, message_id, "Queued for download…")
        .await?;

    let registry = services.state.tasks.clone();
    let downloader = services.downloader.clone();
    let bot_clone = bot.clone();
    let context = RunDownloadContext {
        task_id,
        chat_id,
        message_id,
        url,
        title,
        author,
        selection,
        target,
        cancel,
    };
    tokio::spawn(
        async move {
            run_download_task(bot_clone, registry, downloader, context).await;
        }
        .in_current_span(),
    );

    Ok(())
}

#[instrument(skip(bot, services, q))]
async fn handle_cancel_task(
    bot: &Bot,
    services: AppServices,
    q: CallbackQuery,
    message_id: MessageId,
    chat_id: ChatId,
    task_id: TaskId,
) -> Result<(), AppError> {
    let Some(task) = services.state.tasks.get(&task_id) else {
        warn!(event = "task_missing", task_id = %task_id);
        bot.edit_message_text(chat_id, message_id, "Task expired. Try again.")
            .await?;
        return Ok(());
    };

    let mut remove_now = false;
    let mut mismatch = false;
    {
        let mut task = task.lock().await;
        if task.message_id != message_id || task.chat_id != chat_id {
            mismatch = true;
        } else {
            info!(event = "task_cancelled", task_id = %task_id);
            task.cancel.store(true, Ordering::SeqCst);
            remove_now = matches!(
                task.state,
                TaskState::WaitingFormat { .. } | TaskState::SelectingTarget { .. }
            );
            task.state = TaskState::Finished {
                outcome: TaskOutcome::Cancelled,
            };
        }
    }

    if mismatch {
        warn!(event = "task_message_mismatch", task_id = %task_id);
        bot.edit_message_text(chat_id, message_id, "Task mismatch. Try again.")
            .await?;
        return Ok(());
    }

    bot.answer_callback_query(q.id).await?;
    if remove_now {
        let _ = services.state.tasks.remove(&task_id);
    }
    let _ = bot
        .edit_message_text(chat_id, message_id, "Cancelled.")
        .await;
    Ok(())
}

#[instrument(skip(bot, registry, downloader, context))]
async fn run_download_task(
    bot: Bot,
    registry: TaskRegistry,
    downloader: Arc<dyn Downloader>,
    context: RunDownloadContext,
) {
    let RunDownloadContext {
        task_id,
        chat_id,
        message_id,
        url,
        title,
        author,
        selection,
        target,
        cancel,
    } = context;
    let _guard = registry.guard(task_id.clone());
    if is_cancelled(&cancel) {
        info!(event = "download_task_cancelled_early", task_id = %task_id);
        let _ = bot
            .edit_message_text(chat_id, message_id, "Cancelled.")
            .await;
        return;
    }
    set_task_state(
        &registry,
        &task_id,
        TaskState::Downloading {
            selection: selection.clone(),
            target: target.clone(),
        },
    )
    .await;

    let progress = ProgressState::new();
    let cancel_keyboard = cancel_keyboard(&task_id);
    let _ = bot
        .edit_message_text(chat_id, message_id, "Downloading…")
        .reply_markup(cancel_keyboard.clone())
        .await;
    let (done_tx, progress_task) = spawn_progress_task(ProgressTaskContext {
        bot: bot.clone(),
        chat_id,
        message_id,
        progress: progress.clone(),
        title: title.clone(),
        phase: "Downloading",
        cancel: cancel.clone(),
        keyboard: cancel_keyboard.clone(),
    });

    let result = downloader
        .download(DownloadRequest {
            url,
            format_id: selection.format_id.clone(),
            progress,
            cancel: cancel.clone(),
        })
        .await;

    let _ = done_tx.send(true);
    let _ = progress_task.await;

    let download_result = match result {
        Ok(result) => result,
        Err(AppError::Cancelled) => {
            info!(event = "download_cancelled", task_id = %task_id);
            set_task_state(
                &registry,
                &task_id,
                TaskState::Finished {
                    outcome: TaskOutcome::Cancelled,
                },
            )
            .await;
            let _ = bot
                .edit_message_text(chat_id, message_id, "Cancelled.")
            .await;
            return;
        }
        Err(err) => {
            error!(event = "download_failed", task_id = %task_id, error = %err);
            let err_text = err.to_string();
            let _ = report_user_error(&bot, chat_id, message_id, "Download failed.", &err).await;
            set_task_state(
                &registry,
                &task_id,
                TaskState::Finished {
                    outcome: TaskOutcome::Failed(err_text),
                },
            )
            .await;
            return;
        }
    };

    let output_filename = build_filename(
        title.clone(),
        author.clone(),
        Some(target.ext.clone()),
        &selection.format_id,
    );

    if is_cancelled(&cancel) {
        info!(event = "download_cancelled_post_download", task_id = %task_id);
        set_task_state(
            &registry,
            &task_id,
            TaskState::Finished {
                outcome: TaskOutcome::Cancelled,
            },
        )
        .await;
        let _ = bot
            .edit_message_text(chat_id, message_id, "Cancelled.")
            .await;
        return;
    }

    let needs_conversion = selection
        .ext
        .as_deref()
        .map(|ext| !ext.eq_ignore_ascii_case(&target.ext))
        .unwrap_or(true);
    debug!(
        event = "conversion_check",
        task_id = %task_id,
        needs_conversion
    );

    let upload_result = if needs_conversion {
        set_task_state(
            &registry,
            &task_id,
            TaskState::Converting {
                selection: selection.clone(),
                target: target.clone(),
            },
        )
        .await;
        let _ = bot
            .edit_message_text(chat_id, message_id, "Converting…")
            .reply_markup(cancel_keyboard.clone())
            .await;

        let mut cleanup_paths: Vec<PathBuf> = Vec::new();
        let input_path = match download_result {
            DownloadResult::InMemory { data } => {
                let path = temp_file_path();
                if let Err(err) = tokio::fs::write(&path, data).await {
                    error!(
                        event = "prepare_tempfile_failed",
                        task_id = %task_id,
                        path = %path.display(),
                        error = %err
                    );
                    let _ = report_user_error(
                        &bot,
                        chat_id,
                        message_id,
                        "Failed to prepare file.",
                        err,
                    )
                    .await;
                    return;
                }
                cleanup_paths.push(path.clone());
                path
            }
            DownloadResult::TempFile { path } => {
                debug!(
                    event = "download_tempfile_ready",
                    task_id = %task_id,
                    path = %path.display()
                );
                cleanup_paths.push(path.clone());
                path
            }
        };

        let output_path = temp_file_path_with_ext(&target.ext);
        cleanup_paths.push(output_path.clone());

        let convert_progress = ProgressState::new();
        let (convert_done_tx, convert_progress_task) = spawn_progress_task(ProgressTaskContext {
            bot: bot.clone(),
            chat_id,
            message_id,
            progress: convert_progress.clone(),
            title: title.clone(),
            phase: "Converting",
            cancel: cancel.clone(),
            keyboard: cancel_keyboard.clone(),
        });
        let target_kind = target_kind_from_ext(selection.kind, &target.ext);
        let convert_result = convert_with_ffmpeg(
            &input_path,
            &output_path,
            target_kind,
            convert_progress,
            cancel.clone(),
        )
        .await;
        let _ = convert_done_tx.send(true);
        let _ = convert_progress_task.await;

        if let Err(err) = convert_result {
            if matches!(err, AppError::Cancelled) {
                info!(event = "conversion_cancelled", task_id = %task_id);
                set_task_state(
                    &registry,
                    &task_id,
                    TaskState::Finished {
                        outcome: TaskOutcome::Cancelled,
                    },
                )
                .await;
                let _ = bot
                    .edit_message_text(chat_id, message_id, "Cancelled.")
                    .await;
                cleanup_paths_cleanup(cleanup_paths).await;
                return;
            }
            error!(event = "conversion_failed", task_id = %task_id, error = %err);
            let err_text = err.to_string();
            let _ = report_user_error(&bot, chat_id, message_id, "Conversion failed.", &err).await;
            set_task_state(
                &registry,
                &task_id,
                TaskState::Finished {
                    outcome: TaskOutcome::Failed(err_text),
                },
            )
            .await;
            cleanup_paths_cleanup(cleanup_paths).await;
            return;
        }

        set_task_state(
            &registry,
            &task_id,
            TaskState::Uploading {
                selection: selection.clone(),
                target: target.clone(),
            },
        )
        .await;
        if is_cancelled(&cancel) {
            info!(event = "upload_cancelled_pre_start", task_id = %task_id);
            let _ = bot
                .edit_message_text(chat_id, message_id, "Cancelled.")
                .await;
            cleanup_paths_cleanup(cleanup_paths).await;
            set_task_state(
                &registry,
                &task_id,
                TaskState::Finished {
                    outcome: TaskOutcome::Cancelled,
                },
            )
            .await;
            return;
        }
        if let Err(err) = bot
            .edit_message_text(chat_id, message_id, "Uploading…")
            .await
        {
            error!(
                event = "upload_status_update_failed",
                task_id = %task_id,
                error = %err
            );
            let _ =
                report_user_error(&bot, chat_id, message_id, "Failed to update status.", err).await;
            cleanup_paths_cleanup(cleanup_paths).await;
            return;
        }

        let path_for_upload = output_path.clone();
        let filename_for_upload = output_filename.clone();
        let result = upload_with_progress(&bot, chat_id, message_id, move || {
            InputFile::file(path_for_upload.clone()).file_name(filename_for_upload.clone())
        })
        .await;
        cleanup_paths_cleanup(cleanup_paths).await;
        result
    } else {
        set_task_state(
            &registry,
            &task_id,
            TaskState::Uploading {
                selection: selection.clone(),
                target: target.clone(),
            },
        )
        .await;
        if is_cancelled(&cancel) {
            info!(event = "upload_cancelled_pre_start", task_id = %task_id);
            let _ = bot
                .edit_message_text(chat_id, message_id, "Cancelled.")
                .await;
            set_task_state(
                &registry,
                &task_id,
                TaskState::Finished {
                    outcome: TaskOutcome::Cancelled,
                },
            )
            .await;
            return;
        }
        if let Err(err) = bot
            .edit_message_text(chat_id, message_id, "Uploading…")
            .await
        {
            error!(
                event = "upload_status_update_failed",
                task_id = %task_id,
                error = %err
            );
            let _ =
                report_user_error(&bot, chat_id, message_id, "Failed to update status.", err).await;
            return;
        }
        match download_result {
            DownloadResult::InMemory { data } => {
                debug!(event = "upload_in_memory", task_id = %task_id);
                let data = bytes::Bytes::from(data);
                let filename = output_filename.clone();
                upload_with_progress(&bot, chat_id, message_id, move || {
                    InputFile::memory(data.clone()).file_name(filename.clone())
                })
                .await
            }
            DownloadResult::TempFile { path } => {
                debug!(
                    event = "upload_temp_file",
                    task_id = %task_id,
                    path = %path.display()
                );
                let path_for_upload = path.clone();
                let filename_for_upload = output_filename.clone();
                let result = upload_with_progress(&bot, chat_id, message_id, move || {
                    InputFile::file(path_for_upload.clone()).file_name(filename_for_upload.clone())
                })
                .await;
                let _ = tokio::fs::remove_file(path).await;
                result
            }
        }
    };

    match upload_result {
        Ok(()) => {
            info!(event = "upload_complete", task_id = %task_id);
            if let Err(err) = bot.edit_message_text(chat_id, message_id, "Done.").await {
                error!(
                    event = "upload_complete_status_failed",
                    task_id = %task_id,
                    error = %err
                );
                let err_text = err.to_string();
                let _ =
                    report_user_error(&bot, chat_id, message_id, "Failed to update status.", &err)
                        .await;
                set_task_state(
                    &registry,
                    &task_id,
                    TaskState::Finished {
                        outcome: TaskOutcome::Failed(err_text),
                    },
                )
                .await;
                return;
            }
            set_task_state(
                &registry,
                &task_id,
                TaskState::Finished {
                    outcome: TaskOutcome::Success,
                },
            )
            .await;
        }
        Err(err) => {
            error!(event = "upload_failed", task_id = %task_id, error = %err);
            let err_text = err.to_string();
            let _ = report_user_error(&bot, chat_id, message_id, "Upload failed.", &err).await;
            set_task_state(
                &registry,
                &task_id,
                TaskState::Finished {
                    outcome: TaskOutcome::Failed(err_text),
                },
            )
            .await;
        }
    }
}

#[instrument(skip(registry))]
async fn set_task_state(registry: &TaskRegistry, task_id: &TaskId, state: TaskState) {
    if let Some(task) = registry.get(task_id) {
        let mut task = task.lock().await;
        task.state = state;
    } else {
        warn!(event = "task_state_missing", task_id = %task_id);
    }
}

#[instrument(skip(bot, make_file))]
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
    let progress_task = tokio::spawn(
        async move {
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
                            warn!(event = "upload_progress_edit_failed", error = %err);
                        }
                    }
                }
                _ = done_rx.changed() => {
                    break;
                }
            }
        }
    }
    .in_current_span(),
    );

    let result = send_document_with_retry(bot, chat_id, make_file).await;
    let _ = done_tx.send(true);
    let _ = progress_task.await;
    result
}

#[instrument(skip(bot, make_file))]
async fn send_document_with_retry<F>(
    bot: &Bot,
    chat_id: ChatId,
    make_file: F,
) -> Result<(), AppError>
where
    F: Fn() -> InputFile,
{
    for attempt in 1..=UPLOAD_MAX_RETRIES {
        match bot.send_document(chat_id, make_file()).await {
            Ok(_) => {
                info!(event = "upload_attempt_success", attempt);
                return Ok(());
            }
            Err(err) => {
                let Some(delay) = retry_delay_for_upload(&err, attempt) else {
                    error!(event = "upload_attempt_fatal", attempt, error = %err);
                    return Err(err.into());
                };
                if attempt == UPLOAD_MAX_RETRIES {
                    error!(event = "upload_retry_exhausted", attempt, error = %err);
                    return Err(err.into());
                }
                if delay > Duration::ZERO {
                    warn!(
                        event = "upload_attempt_retry_delay",
                        attempt,
                        delay = ?delay,
                        error = %err
                    );
                    time::sleep(delay).await;
                } else {
                    warn!(
                        event = "upload_attempt_retry_immediate",
                        attempt,
                        error = %err
                    );
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

fn is_cancelled(cancel: &Arc<AtomicBool>) -> bool {
    cancel.load(Ordering::SeqCst)
}

fn cancel_keyboard(task_id: &TaskId) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
        "Cancel",
        build_cancel_callback(task_id),
    )]])
}

fn spawn_waiting_format_timeout(
    bot: Bot,
    registry: TaskRegistry,
    task_id: TaskId,
    chat_id: ChatId,
    message_id: MessageId,
) {
    tokio::spawn(
        async move {
            time::sleep(SELECTION_TIMEOUT).await;
            expire_task_if_state(&bot, &registry, &task_id, chat_id, message_id, |state| {
                matches!(state, TaskState::WaitingFormat { .. })
            })
            .await;
        }
        .in_current_span(),
    );
}

fn spawn_target_selection_timeout(
    bot: Bot,
    registry: TaskRegistry,
    task_id: TaskId,
    chat_id: ChatId,
    message_id: MessageId,
) {
    tokio::spawn(
        async move {
            time::sleep(SELECTION_TIMEOUT).await;
            expire_task_if_state(&bot, &registry, &task_id, chat_id, message_id, |state| {
                matches!(state, TaskState::SelectingTarget { .. })
            })
            .await;
        }
        .in_current_span(),
    );
}

#[instrument(skip(bot, registry, matches_state))]
async fn expire_task_if_state(
    bot: &Bot,
    registry: &TaskRegistry,
    task_id: &TaskId,
    chat_id: ChatId,
    message_id: MessageId,
    matches_state: impl FnOnce(&TaskState) -> bool,
) {
    let Some(task) = registry.get(task_id) else {
        warn!(event = "expire_task_missing", task_id = %task_id);
        return;
    };

    let should_expire = {
        let mut task = task.lock().await;
        if task.message_id != message_id || task.chat_id != chat_id {
            warn!(event = "expire_task_mismatch", task_id = %task_id);
            false
        } else if matches_state(&task.state) {
            task.state = TaskState::Finished {
                outcome: TaskOutcome::Failed("Selection timed out".to_string()),
            };
            true
        } else {
            debug!(event = "expire_task_state_no_match", task_id = %task_id);
            false
        }
    };

    if !should_expire {
        debug!(event = "expire_task_skip", task_id = %task_id);
        return;
    }

    info!(event = "task_expired", task_id = %task_id);
    let _ = registry.remove(task_id);
    let _ = bot
        .edit_message_text(
            chat_id,
            message_id,
            "Selection timed out. Send the link again.",
        )
        .await;
}

struct ProgressTaskContext {
    bot: Bot,
    chat_id: ChatId,
    message_id: MessageId,
    progress: ProgressState,
    title: Option<String>,
    phase: &'static str,
    cancel: Arc<AtomicBool>,
    keyboard: InlineKeyboardMarkup,
}

fn spawn_progress_task(
    context: ProgressTaskContext,
) -> (
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let ProgressTaskContext {
        bot,
        chat_id,
        message_id,
        progress,
        title,
        phase,
        cancel,
        keyboard,
    } = context;
    let (done_tx, mut done_rx) = tokio::sync::watch::channel(false);
    let bot_clone = bot.clone();
    let progress_clone = progress.clone();
    let progress_title = title.clone();
    let phase_label = phase.to_string();
    let keyboard = keyboard.clone();
    let progress_task = tokio::spawn(
        async move {
        let mut ticker = time::interval(PROGRESS_UPDATE_EVERY);
        let mut notified = false;
        let mut last_text: Option<String> = None;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if is_cancelled(&cancel) {
                        break;
                    }
                    let text = progress_clone.build_text(&phase_label, &progress_title).await;
                    if last_text.as_deref() == Some(text.as_str()) {
                        continue;
                    }
                    match bot_clone
                        .edit_message_text(chat_id, message_id, text.clone())
                        .reply_markup(keyboard.clone())
                        .await
                    {
                        Ok(_) => {
                            last_text = Some(text);
                        }
                        Err(err) => {
                            if is_message_not_modified(&err) {
                                last_text = Some(text);
                                continue;
                            }
                            warn!(event = "progress_edit_failed", error = %err);
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
    }
    .in_current_span(),
    );
    (done_tx, progress_task)
}

#[instrument(skip(reader, progress))]
async fn stream_process_output<R>(reader: R, progress: ProgressState) -> Result<(), AppError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reader = tokio::io::BufReader::new(reader);
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];

    loop {
        let read = reader.read(&mut chunk).await.map_err(AppError::Io)?;
        if read == 0 {
            debug!(event = "stream_output_eof");
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        while let Some(pos) = buffer.iter().position(|&b| b == b'\n' || b == b'\r') {
            let mut line = buffer.drain(..=pos).collect::<Vec<u8>>();
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line).to_string();
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                progress.push_line(trimmed.to_string()).await;
                trace!(event = "stream_output_line", line = trimmed);
            }
        }
    }

    if !buffer.is_empty() {
        let line = String::from_utf8_lossy(&buffer).to_string();
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            progress.push_line(trimmed.to_string()).await;
            trace!(event = "stream_output_line", line = trimmed);
        }
    }

    Ok(())
}

#[instrument(skip(reader, progress))]
async fn stream_process_output_with_capture<R>(
    reader: R,
    progress: ProgressState,
    max_bytes: usize,
) -> Result<String, AppError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reader = tokio::io::BufReader::new(reader);
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];
    let mut captured = String::new();

    loop {
        let read = reader.read(&mut chunk).await.map_err(AppError::Io)?;
        if read == 0 {
            debug!(event = "stream_output_capture_eof");
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        while let Some(pos) = buffer.iter().position(|&b| b == b'\n' || b == b'\r') {
            let mut line = buffer.drain(..=pos).collect::<Vec<u8>>();
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line).to_string();
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            progress.push_line(trimmed.to_string()).await;
            trace!(event = "stream_output_capture_line", line = trimmed);
            if captured.len() < max_bytes {
                if !captured.is_empty() {
                    captured.push('\n');
                }
                let remaining = max_bytes - captured.len();
                if trimmed.len() <= remaining {
                    captured.push_str(trimmed);
                } else if remaining > 0 {
                    captured.push_str(&trimmed[..remaining]);
                }
            }
        }
    }

    if !buffer.is_empty() {
        let line = String::from_utf8_lossy(&buffer).to_string();
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            progress.push_line(trimmed.to_string()).await;
            trace!(event = "stream_output_capture_line", line = trimmed);
            if captured.len() < max_bytes {
                if !captured.is_empty() {
                    captured.push('\n');
                }
                let remaining = max_bytes - captured.len();
                if trimmed.len() <= remaining {
                    captured.push_str(trimmed);
                } else if remaining > 0 {
                    captured.push_str(&trimmed[..remaining]);
                }
            }
        }
    }

    Ok(captured)
}

#[instrument(skip(progress, cancel))]
async fn convert_with_ffmpeg(
    input: &Path,
    output: &Path,
    kind: MediaKind,
    progress: ProgressState,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-y").arg("-i").arg(input);
    match kind {
        MediaKind::Audio => {
            debug!(event = "ffmpeg_audio_mode");
            cmd.arg("-vn");
        }
        MediaKind::Image => {
            debug!(event = "ffmpeg_image_mode");
            cmd.arg("-frames:v").arg("1");
        }
        _ => {}
    }
    cmd.arg(output);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = cmd.spawn().map_err(AppError::Io)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::MissingOutput("ffmpeg stdout".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| AppError::MissingOutput("ffmpeg stderr".into()))?;

    let stdout_task = tokio::spawn(stream_process_output(stdout, progress.clone()).in_current_span());
    let stderr_task = tokio::spawn(
        stream_process_output_with_capture(stderr, progress, 16 * 1024).in_current_span(),
    );

    let status = loop {
        tokio::select! {
            status = child.wait() => {
                break status.map_err(AppError::Io)?;
            }
            _ = time::sleep(Duration::from_millis(200)) => {
                if is_cancelled(&cancel) {
                    info!(event = "ffmpeg_cancelled");
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    let _ = stdout_task.await;
                    let _ = stderr_task.await;
                    return Err(AppError::Cancelled);
                }
            }
        }
    };
    let stdout_result = stdout_task.await?;
    let stderr_result = stderr_task.await?;

    stdout_result?;
    let stderr_text = stderr_result?;

    if !status.success() {
        let trimmed = stderr_text.trim();
        if trimmed.is_empty() {
            error!(event = "ffmpeg_failed_no_output", status = %status);
            return Err(AppError::FfmpegFailed(status));
        }
        error!(event = "ffmpeg_failed", status = %status, stderr = trimmed);
        return Err(AppError::FfmpegFailedWithOutput(
            status,
            trimmed.to_string(),
        ));
    }
    Ok(())
}

#[instrument]
async fn cleanup_paths_cleanup(paths: Vec<PathBuf>) {
    for path in paths {
        debug!(event = "cleanup_path", path = %path.display());
        let _ = tokio::fs::remove_file(path).await;
    }
}

fn temp_file_path_with_ext(ext: &str) -> PathBuf {
    let mut path = temp_file_path();
    let trimmed = ext.trim_start_matches('.');
    if !trimmed.is_empty() {
        path.set_extension(trimmed);
    }
    path
}

fn target_formats_for(kind: MediaKind, source_ext: Option<&str>) -> Vec<TargetFormat> {
    let targets = match kind {
        MediaKind::Audio => vec!["mp3", "m4a", "opus", "wav", "flac"],
        MediaKind::Image => vec!["jpg", "png", "webp"],
        MediaKind::Video => vec!["mp4", "mkv", "webm", "mp3", "m4a", "opus", "wav", "flac"],
        MediaKind::Unknown => vec!["mp4", "mkv", "webm"],
    };
    debug!(event = "target_formats_base", kind = ?kind, source_ext = ?source_ext);
    let mut list: Vec<TargetFormat> = targets
        .into_iter()
        .map(|ext| TargetFormat {
            ext: ext.to_string(),
        })
        .collect();
    if let Some(ext) = source_ext {
        let trimmed = ext.trim();
        let exists = list
            .iter()
            .any(|target| target.ext.eq_ignore_ascii_case(trimmed));
        if !exists && !trimmed.is_empty() {
            list.insert(
                0,
                TargetFormat {
                    ext: trimmed.to_string(),
                },
            );
        }
    }
    list
}

fn target_label(kind: &MediaKind, ext: &str) -> String {
    let tag = match target_kind_from_ext(*kind, ext) {
        MediaKind::Audio => "audio",
        MediaKind::Image => "image",
        MediaKind::Video => "video",
        MediaKind::Unknown => "media",
    };
    trace!(event = "target_label", kind = ?kind, ext = ext, tag = tag);
    format!("{} ({tag})", ext.to_uppercase())
}

fn target_kind_from_ext(source_kind: MediaKind, ext: &str) -> MediaKind {
    if matches!(source_kind, MediaKind::Audio | MediaKind::Image) {
        trace!(event = "target_kind_source_fixed", kind = ?source_kind);
        return source_kind;
    }
    let ext = ext.trim_start_matches('.').to_ascii_lowercase();
    if is_audio_ext(Some(ext.as_str())) {
        trace!(event = "target_kind_audio", ext = ext);
        MediaKind::Audio
    } else if is_image_ext(Some(ext.as_str())) {
        trace!(event = "target_kind_image", ext = ext);
        MediaKind::Image
    } else {
        trace!(event = "target_kind_video", ext = ext);
        MediaKind::Video
    }
}

fn is_audio_ext(ext: Option<&str>) -> bool {
    matches!(
        ext,
        Some("mp3" | "m4a" | "opus" | "wav" | "flac" | "aac" | "ogg")
    )
}

#[instrument(skip(bot, err))]
async fn report_user_error(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    context: &str,
    err: impl Display,
) -> Result<(), AppError> {
    warn!(
        event = "report_user_error",
        chat_id = %chat_id,
        message_id = %message_id,
        context = context,
        error = %err
    );
    let text = format!("{context}\nError: {err}");
    if bot
        .edit_message_text(chat_id, message_id, text.clone())
        .await
        .is_err()
    {
        warn!(event = "report_user_error_fallback", chat_id = %chat_id, message_id = %message_id);
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

fn select_author(info: &YtDlpInfo) -> Option<String> {
    info.uploader
        .clone()
        .or_else(|| info.channel.clone())
        .or_else(|| info.creator.clone())
        .or_else(|| info.artist.clone())
}

fn media_kind(format: &YtDlpFormat) -> MediaKind {
    let vcodec = format.vcodec.as_deref();
    let acodec = format.acodec.as_deref();
    let v_none = matches!(vcodec, None | Some("none"));
    let a_none = matches!(acodec, None | Some("none"));
    let ext = format.ext.as_deref();

    if is_image_ext(ext) && v_none && a_none {
        return MediaKind::Image;
    }
    if v_none && !a_none {
        return MediaKind::Audio;
    }
    if a_none && !v_none {
        return MediaKind::Video;
    }
    if !v_none || !a_none {
        return MediaKind::Video;
    }
    MediaKind::Unknown
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
        let entry = best
            .entry(ext)
            .or_insert_with(|| (key.0, key.1, key.2, format.format_id.clone()));
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
    if let Some(abr) = format.abr.filter(|value| *value > 0.0) {
        return Some(abr);
    }
    if let Some(tbr) = format.tbr.filter(|value| *value > 0.0) {
        return Some(tbr);
    }
    let duration = format.duration?;
    if duration <= 0.0 {
        return None;
    }
    let bytes = format.filesize.or(format.filesize_approx)?;
    let kbps = (bytes as f64 * 8.0) / duration / 1000.0;
    if kbps > 0.0 { Some(kbps) } else { None }
}

fn is_image_ext(ext: Option<&str>) -> bool {
    matches!(
        ext,
        Some(
            "jpg"
                | "jpeg"
                | "png"
                | "webp"
                | "gif"
                | "bmp"
                | "tiff"
                | "tif"
                | "svg"
                | "avif"
                | "heic"
                | "heif"
        )
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

pub fn build_filename(
    title: Option<String>,
    author: Option<String>,
    ext: Option<String>,
    fallback_id: &str,
) -> String {
    let mut name = title.unwrap_or_else(|| fallback_id.to_string());
    name = name
        .chars()
        .map(|c| if is_safe_filename_char(c) { c } else { '_' })
        .collect::<String>();

    let mut author = author.unwrap_or_else(|| "Unknown".to_string());
    author = author
        .chars()
        .map(|c| if is_safe_filename_char(c) { c } else { '_' })
        .collect::<String>();

    if author.trim().is_empty() {
        author = "Unknown".to_string();
    }

    let ext = ext.unwrap_or_else(|| "bin".to_string());
    format!("{} - {}.{}", name.trim(), author.trim(), ext)
}

fn is_safe_filename_char(c: char) -> bool {
    // Keep Unicode (including CJK) and exclude common filesystem-unsafe characters.
    if c.is_control() {
        return false;
    }
    !matches!(
        c,
        '/' | '\\' | '?' | '%' | '*' | ':' | '|' | '"' | '<' | '>'
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_filename_sanitizes_and_adds_ext() {
        let name = build_filename(
            Some("My/Video?".into()),
            Some("A/Author".into()),
            Some("mp3".into()),
            "fallback",
        );
        assert_eq!(name, "My_Video_ - A_Author.mp3");
    }

    #[test]
    fn build_filename_preserves_unicode() {
        let name = build_filename(
            Some("你好 世界".into()),
            Some("作者".into()),
            Some("mp4".into()),
            "fallback",
        );
        assert_eq!(name, "你好 世界 - 作者.mp4");
    }

    #[test]
    fn format_label_includes_best_marker() {
        let format = YtDlpFormat {
            format_id: "18".into(),
            ext: Some("mp4".into()),
            format_note: None,
            height: Some(720),
            width: Some(1280),
            tbr: Some(1000.0),
            abr: None,
            vcodec: Some("avc1".into()),
            acodec: Some("mp4a".into()),
            filesize: Some(1_000_000),
            filesize_approx: None,
            duration: Some(60.0),
        };
        let label = format_label(&format, true);
        assert!(label.starts_with("⭐ "));
    }
}
