use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Teloxide request error: {0}")]
    Teloxide(#[from] teloxide::RequestError),

    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("yt-dlp execution failed: {0}")]
    YtDlp(String),

    #[error("Missing yt-dlp output: {0}")]
    MissingOutput(String),

    #[error("Download failed with exit code: {0}")]
    DownloadFailed(std::process::ExitStatus),

    #[error("Task join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Internal error: {0}")]
    Internal(String),
}
