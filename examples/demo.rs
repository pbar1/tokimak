use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use indicatif::ProgressStyle;
use rand::Rng;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::Span;
use tracing_glog::Glog;
use tracing_glog::GlogFields;
use tracing_glog::LocalTime;
use tracing_indicatif::filter::IndicatifFilter;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;

/// Tokimak task reactor demo
#[derive(Debug, Parser)]
struct Cli {
    /// Total number of tasks
    #[clap(short, long, default_value_t = 10_000_000)]
    tasks: usize,

    /// Number of concurrent tasks to run at once
    #[clap(short, long, default_value_t = 50)]
    concurrency: usize,

    /// Log level for stderr
    #[clap(short, long, default_value = "info", env = "RUST_LOG")]
    log_level: String,
}

impl Cli {
    fn init_tracing(&self) -> Result<()> {
        let indicatif_layer = IndicatifLayer::new()
            .with_progress_style(ProgressStyle::default_bar())
            .with_filter(IndicatifFilter::new(false));

        let stderr_writer = indicatif_layer.inner().get_stderr_writer();
        let stderr_filter = EnvFilter::builder().parse_lossy(&self.log_level);
        let stderr_layer = tracing_subscriber::fmt::layer()
            .with_writer(stderr_writer)
            .event_format(Glog::default().with_timer(LocalTime::default()))
            .fmt_fields(GlogFields::default())
            .with_filter(stderr_filter);

        let console_layer = console_subscriber::spawn();

        let subscriber = tracing_subscriber::registry()
            .with(indicatif_layer)
            .with(stderr_layer)
            .with(console_layer);
        tracing::subscriber::set_global_default(subscriber)?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.init_tracing()?;
    real_main(cli).await?;
    Ok(())
}

#[instrument(skip_all, fields(indicatif.pb_show))]
async fn real_main(cli: Cli) -> Result<()> {
    // Setup progress bar goal
    Span::current().pb_set_length(cli.tasks as u64);

    let metrics = tokio::runtime::Handle::current().metrics();
    info!(
        blocking_threads = metrics.num_blocking_threads(),
        workers = metrics.num_workers(),
    );

    let _outcome = futures::stream::iter(0..cli.tasks)
        .map(do_work_spawn)
        .for_each_concurrent(cli.concurrency, |f| async { f.await })
        .await;

    Ok(())
}

async fn do_work_spawn(n: usize) {
    tokio::task::Builder::new()
        .name(&format!("t-{n}"))
        .spawn(do_work_inner(n))
        .unwrap()
        .await
        .unwrap();

    Span::current().pb_inc(1);
}

async fn do_work_spawn_local(n: usize) {
    tokio::task::Builder::new()
        .name(&format!("t-{n}"))
        .spawn_local(do_work_inner(n))
        .unwrap()
        .await
        .unwrap();

    Span::current().pb_inc(1);
}

/// Simulates some work that takes some time and can time out
async fn do_work_inner(n: usize) {
    let metrics = tokio::runtime::Handle::current().metrics();
    let tasks = metrics.num_alive_tasks();

    let sleep = rand::thread_rng().gen_range(0..20);

    let result = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::time::sleep(Duration::from_secs(sleep)).await;
    })
    .await;

    if n % 10_000 == 0 {
        match result {
            Ok(_ok) => info!(n, sleep, tasks, "success"),
            Err(_err) => error!(n, sleep, tasks, "failure"),
        }
    } else {
        match result {
            Ok(_ok) => debug!(n, sleep, tasks, "success"),
            Err(_err) => debug!(n, sleep, tasks, "failure"),
        }
    }
}
