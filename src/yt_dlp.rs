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
use tracing::{Instrument, debug, error, info, instrument, trace};

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
    #[instrument(skip(self))]
    async fn fetch_formats(&self, url: &str) -> Result<crate::handlers::YtDlpInfo, AppError> {
        info!(event = "fetch_formats_start", url = %url);
        let mut cmd = yt_dlp_base_command();
        cmd.arg("-J").arg("--no-playlist").arg(url);
        let output = cmd.output().await.map_err(AppError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(
                event = "fetch_formats_failed",
                status = %output.status,
                stderr = %stderr.trim()
            );
            return Err(AppError::YtDlp(stderr.trim().to_string()));
        }

        let info: crate::handlers::YtDlpInfo =
            serde_json::from_slice(&output.stdout).map_err(AppError::Json)?;
        info!(
            event = "fetch_formats_success",
            format_count = info.formats.len()
        );
        Ok(info)
    }
}

#[async_trait]
impl Downloader for YtDlpClient {
    #[instrument(skip(self, req))]
    async fn download(&self, req: DownloadRequest) -> Result<DownloadResult, AppError> {
        download_with_progress(req).await
    }
}

#[instrument(skip(req))]
async fn download_with_progress(req: DownloadRequest) -> Result<DownloadResult, AppError> {
    info!(
        event = "download_with_progress_start",
        url = %req.url,
        format_id = %req.format_id
    );
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
        .map_err(AppError::Io)?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::MissingOutput("stdout".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| AppError::MissingOutput("stderr".into()))?;

    let progress = req.progress.clone();
    let stderr_task = tokio::spawn(
        async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                trace!(event = "yt_dlp_stderr_line", line = line.as_str());
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

    loop {
        if req.cancel.load(Ordering::SeqCst) {
            info!(event = "download_cancelled_before_read");
            let _ = child.kill().await;
            let _ = child.wait().await;
            if let Some(path) = temp_path.as_ref() {
                let _ = tokio::fs::remove_file(path).await;
            }
            return Err(AppError::Cancelled);
        }
        let read = reader.read(&mut chunk).await.map_err(AppError::Io)?;
        if read == 0 {
            debug!(event = "download_stdout_eof");
            break;
        }
        if req.cancel.load(Ordering::SeqCst) {
            info!(event = "download_cancelled_during_read");
            let _ = child.kill().await;
            let _ = child.wait().await;
            if let Some(path) = temp_path.as_ref() {
                let _ = tokio::fs::remove_file(path).await;
            }
            return Err(AppError::Cancelled);
        }
        if temp_file.is_none() && buffer.len() + read <= crate::handlers::MAX_IN_MEMORY_BYTES {
            trace!(
                event = "download_buffer_in_memory",
                buffered_bytes = buffer.len() + read
            );
            buffer.extend_from_slice(&chunk[..read]);
        } else {
            if temp_file.is_none() {
                let path = crate::handlers::temp_file_path();
                let mut file = tokio::fs::File::create(&path).await.map_err(AppError::Io)?;
                file.write_all(&buffer).await.map_err(AppError::Io)?;
                buffer.clear();
                temp_file = Some(file);
                temp_path = Some(path);
                info!(event = "download_spill_to_disk");
            }
            if let Some(file) = temp_file.as_mut() {
                file.write_all(&chunk[..read]).await.map_err(AppError::Io)?;
            }
        }
    }

    let status = child.wait().await.map_err(AppError::Io)?;
    let _ = stderr_task.await;

    if !status.success() {
        error!(event = "download_failed_status", status = %status);
        return Err(AppError::DownloadFailed(status));
    }

    if let Some(path) = temp_path {
        info!(event = "download_complete_tempfile", path = %path.display());
        Ok(DownloadResult::TempFile { path })
    } else {
        info!(event = "download_complete_in_memory", bytes = buffer.len());
        Ok(DownloadResult::InMemory {
            data: buffer.to_vec(),
        })
    }
}

fn yt_dlp_base_command() -> Command {
    let mut cmd = Command::new("yt-dlp");
    let player_client = env::var("YTDLP_PLAYER_CLIENT").unwrap_or_else(|_| "android".to_string());
    cmd.arg("--extractor-args")
        .arg(format!("youtube:player_client={player_client}"));

    if matches!(
        env::var("YTDLP_FORCE_IPV4").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) {
        info!(event = "yt_dlp_force_ipv4");
        cmd.arg("--force-ipv4");
    }

    debug!(event = "yt_dlp_command_ready");
    cmd
}
