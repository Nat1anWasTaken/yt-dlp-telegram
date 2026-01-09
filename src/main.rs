mod error;
mod handlers;
mod tasks;
mod yt_dlp;

use crate::error::AppError;
use handlers::{AppServices, build_handler};
use std::time::Duration;
use teloxide::{net::default_reqwest_settings, prelude::*};
use tracing::{error, info, warn, instrument, Instrument};
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    if let Err(err) = run().await {
        eprintln!("fatal error: {err}");
        error!(error = %err, "Application terminated with fatal error");
        return Err(err);
    }
    info!("Application shutdown complete");
    Ok(())
}

#[instrument]
async fn run() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Initializing yt-dlp-telegram bot");
    dotenv::dotenv().ok();

    let client = default_reqwest_settings()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(|e| {
            error!(error = %e, "Failed to build HTTP client");
            e
        })?;
    info!("HTTP client configured successfully");

    let bot = Bot::from_env_with_client(client);
    info!("Telegram bot initialized");

    let handler = build_handler();
    let services = AppServices::new();
    info!("Application services initialized");

    let mut dispatcher = Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![services])
        .build();
    info!("Dispatcher built successfully");

    let shutdown_token = dispatcher.shutdown_token();
    tokio::spawn(
        async move {
            info!("Shutdown signal handler spawned");
            shutdown_signal().await;
            info!("Shutdown signal received, initiating graceful shutdown");
            if let Ok(wait) = shutdown_token.shutdown() {
                wait.await;
                info!("Graceful shutdown completed");
            } else {
                warn!("Failed to initiate graceful shutdown");
            }
        }
        .in_current_span(),
    );

    info!("Starting dispatcher event loop");
    dispatcher.dispatch().await;
    info!("Dispatcher stopped");
    Ok(())
}

#[cfg(unix)]
#[instrument]
async fn shutdown_signal() {
    info!("Setting up signal handlers for graceful shutdown");
    let term = signal(SignalKind::terminate());
    let interrupt = signal(SignalKind::interrupt());
    match (term, interrupt) {
        (Ok(mut term), Ok(mut interrupt)) => {
            info!("SIGTERM and SIGINT handlers registered successfully");
            tokio::select! {
                _ = term.recv() => {
                    info!("Received SIGTERM signal");
                }
                _ = interrupt.recv() => {
                    info!("Received SIGINT signal");
                }
            }
        }
        (Ok(mut term), Err(err)) => {
            warn!(error = %err, "Failed to register SIGINT handler, falling back to SIGTERM only");
            let _ = term.recv().await;
            info!("Received SIGTERM signal");
        }
        (Err(err), Ok(mut interrupt)) => {
            warn!(error = %err, "Failed to register SIGTERM handler, falling back to SIGINT only");
            let _ = interrupt.recv().await;
            info!("Received SIGINT signal");
        }
        (Err(term_err), Err(int_err)) => {
            error!(sigterm_error = %term_err, sigint_error = %int_err, "Failed to register both SIGTERM and SIGINT handlers");
            warn!("Falling back to Ctrl+C handler");
            let _ = tokio::signal::ctrl_c().await;
            info!("Received Ctrl+C");
        }
    }
}

#[cfg(not(unix))]
#[instrument]
async fn shutdown_signal() {
    info!("Waiting for Ctrl+C signal");
    let _ = tokio::signal::ctrl_c().await;
    info!("Received Ctrl+C signal");
}
