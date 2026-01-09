mod handlers;
mod yt_dlp;

use handlers::{build_handler, AppServices};
use teloxide::prelude::*;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    dotenv::dotenv().ok();

    let bot = Bot::from_env();
    let handler = build_handler();
    let services = AppServices::new();

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![services])
        .build()
        .dispatch()
        .await;
}
