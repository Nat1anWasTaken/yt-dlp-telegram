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
    async fn fetch_formats(&self, url: &str) -> Result<crate::handlers::YtDlpInfo, AppError> {
        let mut cmd = yt_dlp_base_command();
        cmd.arg("-J").arg("--no-playlist").arg(url);
        let output = cmd.output().await.map_err(AppError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(AppError::YtDlp(stderr.trim().to_string()));
        }

        let info: crate::handlers::YtDlpInfo =
            serde_json::from_slice(&output.stdout).map_err(AppError::Json)?;
        Ok(info)
    }
}

#[async_trait]
impl Downloader for YtDlpClient {
    async fn download(&self, req: DownloadRequest) -> Result<DownloadResult, AppError> {
        download_with_progress(req).await
    }
}

async fn download_with_progress(req: DownloadRequest) -> Result<DownloadResult, AppError> {
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
    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            progress.push_line(line).await;
        }
    });

    let mut buffer = BytesMut::new();
    let mut temp_path: Option<std::path::PathBuf> = None;
    let mut temp_file: Option<tokio::fs::File> = None;
    let mut reader = BufReader::new(stdout);
    let mut chunk = vec![0u8; 64 * 1024];

    loop {
        if req.cancel.load(Ordering::SeqCst) {
            let _ = child.kill().await;
            let _ = child.wait().await;
            if let Some(path) = temp_path.as_ref() {
                let _ = tokio::fs::remove_file(path).await;
            }
            return Err(AppError::Cancelled);
        }
        let read = reader.read(&mut chunk).await.map_err(AppError::Io)?;
        if read == 0 {
            break;
        }
        if req.cancel.load(Ordering::SeqCst) {
            let _ = child.kill().await;
            let _ = child.wait().await;
            if let Some(path) = temp_path.as_ref() {
                let _ = tokio::fs::remove_file(path).await;
            }
            return Err(AppError::Cancelled);
        }
        if temp_file.is_none() && buffer.len() + read <= crate::handlers::MAX_IN_MEMORY_BYTES {
            buffer.extend_from_slice(&chunk[..read]);
        } else {
            if temp_file.is_none() {
                let path = crate::handlers::temp_file_path();
                let mut file = tokio::fs::File::create(&path).await.map_err(AppError::Io)?;
                file.write_all(&buffer).await.map_err(AppError::Io)?;
                buffer.clear();
                temp_file = Some(file);
                temp_path = Some(path);
            }
            if let Some(file) = temp_file.as_mut() {
                file.write_all(&chunk[..read]).await.map_err(AppError::Io)?;
            }
        }
    }

    let status = child.wait().await.map_err(AppError::Io)?;
    let _ = stderr_task.await;

    if !status.success() {
        return Err(AppError::DownloadFailed(status));
    }

    if let Some(path) = temp_path {
        Ok(DownloadResult::TempFile { path })
    } else {
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
        cmd.arg("--force-ipv4");
    }

    cmd
}
