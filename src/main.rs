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
use tracing::{Instrument, error, info, instrument, warn};

#[tokio::main]
#[instrument]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    if let Err(err) = run().await {
        eprintln!("fatal error: {err}");
        error!(event = "fatal_error", error = %err);
        return Err(err);
    }
    Ok(())
}

#[instrument]
async fn run() -> Result<(), AppError> {
    dotenv::dotenv().ok();
    info!(event = "startup_begin");

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
    tokio::spawn(
        async move {
            shutdown_signal().await;
            if let Ok(wait) = shutdown_token.shutdown() {
                wait.await;
            }
        }
        .in_current_span(),
    );

    dispatcher.dispatch().await;
    info!(event = "dispatcher_exit");
    Ok(())
}

#[cfg(unix)]
#[instrument]
async fn shutdown_signal() {
    let term = signal(SignalKind::terminate());
    let interrupt = signal(SignalKind::interrupt());
    match (term, interrupt) {
        (Ok(mut term), Ok(mut interrupt)) => {
            info!(
                event = "shutdown_signal_listening",
                signal = "term_or_interrupt"
            );
            tokio::select! {
                _ = term.recv() => {}
                _ = interrupt.recv() => {}
            }
        }
        (Ok(mut term), Err(err)) => {
            warn!(event = "signal_listen_failed", signal = "SIGINT", error = %err);
            let _ = term.recv().await;
        }
        (Err(err), Ok(mut interrupt)) => {
            warn!(event = "signal_listen_failed", signal = "SIGTERM", error = %err);
            let _ = interrupt.recv().await;
        }
        (Err(term_err), Err(int_err)) => {
            warn!(
                event = "signal_listen_failed",
                signal = "SIGTERM",
                error = %term_err
            );
            warn!(
                event = "signal_listen_failed",
                signal = "SIGINT",
                error = %int_err
            );
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
#[instrument]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
