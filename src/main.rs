mod error;
mod handlers;
mod tasks;
mod yt_dlp;

use crate::error::AppError;
use handlers::{build_handler, AppServices};
use std::time::Duration;
use teloxide::{net::default_reqwest_settings, prelude::*};
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    if let Err(err) = run().await {
        eprintln!("fatal error: {err}");
        tracing::error!("fatal error: {err}");
        return Err(err);
    }
    Ok(())
}

async fn run() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    dotenv::dotenv().ok();

    let client = default_reqwest_settings()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()?;
    let bot = Bot::from_env_with_client(client);
    let handler = build_handler();
    let services = AppServices::new();

    let mut dispatcher = Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![services])
        .build();
    let shutdown_token = dispatcher.shutdown_token();
    tokio::spawn(async move {
        shutdown_signal().await;
        if let Ok(wait) = shutdown_token.shutdown() {
            wait.await;
        }
    });

    dispatcher.dispatch().await;
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() {
    let mut term = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
    let mut interrupt = signal(SignalKind::interrupt()).expect("failed to listen for SIGINT");
    tokio::select! {
        _ = term.recv() => {}
        _ = interrupt.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
