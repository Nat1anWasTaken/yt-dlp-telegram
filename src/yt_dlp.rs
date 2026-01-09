use crate::{
    error::AppError,
    handlers::{DownloadRequest, DownloadResult},
};
use async_trait::async_trait;
use bytes::BytesMut;
use std::env;
use std::sync::atomic::Ordering;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
};
use tracing::{debug, error, info, warn, instrument, Instrument};

#[derive(Default)]
pub struct YtDlpClient;

#[async_trait]
pub trait MediaProvider: Send + Sync {
    async fn fetch_formats(&self, url: &str) -> Result<crate::handlers::YtDlpInfo, AppError>;
}

#[async_trait]
pub trait Downloader: Send + Sync {
    async fn download(&self, req: DownloadRequest) -> Result<DownloadResult, AppError>;
}

#[async_trait]
impl MediaProvider for YtDlpClient {
    #[instrument(skip(self), fields(url = %url))]
    async fn fetch_formats(&self, url: &str) -> Result<crate::handlers::YtDlpInfo, AppError> {
        info!(url = %url, "Fetching media formats with yt-dlp");
        let mut cmd = yt_dlp_base_command();
        cmd.arg("-J").arg("--no-playlist").arg(url);

        debug!("Executing yt-dlp command");
        let output = cmd.output().await.map_err(|e| {
            error!(error = %e, "Failed to execute yt-dlp");
            AppError::Io(e)
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(exit_code = ?output.status.code(), stderr = %stderr, "yt-dlp failed");
            return Err(AppError::YtDlp(stderr.trim().to_string()));
        }

        debug!(stdout_size = output.stdout.len(), "Parsing yt-dlp JSON output");
        let info: crate::handlers::YtDlpInfo =
            serde_json::from_slice(&output.stdout).map_err(|e| {
                error!(error = %e, "Failed to parse yt-dlp JSON output");
                AppError::Json(e)
            })?;

        info!(format_count = info.formats.len(), title = ?info.title, "Successfully fetched media formats");
        Ok(info)
    }
}

#[async_trait]
impl Downloader for YtDlpClient {
    #[instrument(skip(self, req), fields(url = %req.url, format_id = %req.format_id))]
    async fn download(&self, req: DownloadRequest) -> Result<DownloadResult, AppError> {
        info!(url = %req.url, format_id = %req.format_id, "Starting download");
        download_with_progress(req).await
    }
}

#[instrument(skip(req), fields(url = %req.url, format_id = %req.format_id))]
async fn download_with_progress(req: DownloadRequest) -> Result<DownloadResult, AppError> {
    debug!("Building yt-dlp download command");
    let mut cmd = yt_dlp_base_command();
    let mut child = cmd
        .arg("-f")
        .arg(&req.format_id)
        .arg("-o")
        .arg("-")
        .arg("--no-playlist")
        .arg("--newline")
        .arg("--progress")
        .arg(&req.url)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| {
            error!(error = %e, "Failed to spawn yt-dlp process");
            AppError::Io(e)
        })?;

    info!("yt-dlp process spawned successfully");

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::MissingOutput("stdout".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| AppError::MissingOutput("stderr".into()))?;

    let progress = req.progress.clone();
    debug!("Spawning stderr reader task");
    let stderr_task = tokio::spawn(
        async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                progress.push_line(line).await;
            }
        }
        .in_current_span(),
    );

    let mut buffer = BytesMut::new();
    let mut temp_path: Option<std::path::PathBuf> = None;
    let mut temp_file: Option<tokio::fs::File> = None;
    let mut reader = BufReader::new(stdout);
    let mut chunk = vec![0u8; 64 * 1024];

    debug!("Starting download loop");
    loop {
        if req.cancel.load(Ordering::SeqCst) {
            warn!("Download cancelled during read loop");
            let _ = child.kill().await;
            let _ = child.wait().await;
            if let Some(path) = temp_path.as_ref() {
                debug!(path = %path.display(), "Cleaning up temp file after cancellation");
                let _ = tokio::fs::remove_file(path).await;
            }
            return Err(AppError::Cancelled);
        }
        let read = reader.read(&mut chunk).await.map_err(|e| {
            error!(error = %e, "Failed to read from yt-dlp stdout");
            AppError::Io(e)
        })?;
        if read == 0 {
            debug!("Reached end of yt-dlp stdout");
            break;
        }
        if req.cancel.load(Ordering::SeqCst) {
            warn!("Download cancelled during read loop");
            let _ = child.kill().await;
            let _ = child.wait().await;
            if let Some(path) = temp_path.as_ref() {
                debug!(path = %path.display(), "Cleaning up temp file after cancellation");
                let _ = tokio::fs::remove_file(path).await;
            }
            return Err(AppError::Cancelled);
        }
        if temp_file.is_none() && buffer.len() + read <= crate::handlers::MAX_IN_MEMORY_BYTES {
            buffer.extend_from_slice(&chunk[..read]);
        } else {
            if temp_file.is_none() {
                info!(current_size = buffer.len(), max_in_memory = crate::handlers::MAX_IN_MEMORY_BYTES,
                      "Exceeded in-memory limit, switching to temp file");
                let path = crate::handlers::temp_file_path();
                debug!(path = %path.display(), "Creating temp file");
                let mut file = tokio::fs::File::create(&path).await.map_err(|e| {
                    error!(error = %e, path = %path.display(), "Failed to create temp file");
                    AppError::Io(e)
                })?;
                file.write_all(&buffer).await.map_err(|e| {
                    error!(error = %e, "Failed to write to temp file");
                    AppError::Io(e)
                })?;
                buffer.clear();
                temp_file = Some(file);
                temp_path = Some(path);
            }
            if let Some(file) = temp_file.as_mut() {
                file.write_all(&chunk[..read]).await.map_err(|e| {
                    error!(error = %e, "Failed to write to temp file");
                    AppError::Io(e)
                })?;
            }
        }
    }

    debug!("Waiting for yt-dlp process to complete");
    let status = child.wait().await.map_err(|e| {
        error!(error = %e, "Failed to wait for yt-dlp process");
        AppError::Io(e)
    })?;
    let _ = stderr_task.await;

    if !status.success() {
        error!(exit_code = ?status.code(), "yt-dlp download failed");
        return Err(AppError::DownloadFailed(status));
    }

    if let Some(path) = temp_path {
        info!(path = %path.display(), "Download completed to temp file");
        Ok(DownloadResult::TempFile { path })
    } else {
        info!(size_bytes = buffer.len(), "Download completed in memory");
        Ok(DownloadResult::InMemory {
            data: buffer.to_vec(),
        })
    }
}

fn yt_dlp_base_command() -> Command {
    let mut cmd = Command::new("yt-dlp");
    let player_client = env::var("YTDLP_PLAYER_CLIENT").unwrap_or_else(|_| "android".to_string());
    debug!(player_client = %player_client, "Configuring yt-dlp player client");
    cmd.arg("--extractor-args")
        .arg(format!("youtube:player_client={player_client}"));

    if matches!(
        env::var("YTDLP_FORCE_IPV4").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) {
        debug!("Force IPv4 enabled for yt-dlp");
        cmd.arg("--force-ipv4");
    }

    cmd
}
