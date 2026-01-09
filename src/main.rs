mod error;
mod handlers;
mod tasks;
mod yt_dlp;

use crate::error::AppError;
use handlers::{AppServices, build_handler};
use std::time::Duration;
use teloxide::{net::default_reqwest_settings, prelude::*};
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

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
    let term = signal(SignalKind::terminate());
    let interrupt = signal(SignalKind::interrupt());
    match (term, interrupt) {
        (Ok(mut term), Ok(mut interrupt)) => {
            tokio::select! {
                _ = term.recv() => {}
                _ = interrupt.recv() => {}
            }
        }
        (Ok(mut term), Err(err)) => {
            tracing::warn!("failed to listen for SIGINT: {err}");
            let _ = term.recv().await;
        }
        (Err(err), Ok(mut interrupt)) => {
            tracing::warn!("failed to listen for SIGTERM: {err}");
            let _ = interrupt.recv().await;
        }
        (Err(term_err), Err(int_err)) => {
            tracing::warn!("failed to listen for SIGTERM: {term_err}");
            tracing::warn!("failed to listen for SIGINT: {int_err}");
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
