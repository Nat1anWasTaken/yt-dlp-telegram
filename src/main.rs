mod error;
mod handlers;
mod yt_dlp;

use crate::error::AppError;
use handlers::{build_handler, AppServices};
use std::time::Duration;
use teloxide::{net::default_reqwest_settings, prelude::*};

#[tokio::main]
async fn main() -> Result<(), AppError> {
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

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![services])
        .build()
        .dispatch()
        .await;

    Ok(())
}
