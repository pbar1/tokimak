use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use futures::StreamExt;
use indicatif::ProgressStyle;
use rand::Rng;
use tokimak::TaskReactor;
use tokio::io::AsyncWriteExt;
use tokio::net::ToSocketAddrs;
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

static ACTIVE: AtomicUsize = AtomicUsize::new(0);

#[instrument(skip_all, fields(indicatif.pb_show))]
async fn real_main(cli: Cli) -> Result<()> {
    // Setup progress bar goal
    Span::current().pb_set_length(cli.tasks as u64);

    let metrics = tokio::runtime::Handle::current().metrics();
    info!(
        blocking_threads = metrics.num_blocking_threads(),
        workers = metrics.num_workers(),
    );

    let stream = futures::stream::iter(0..cli.tasks).map(do_work);

    let mut tasks = TaskReactor::buffer_spawned(cli.concurrency, stream);

    while let Some(result) = tasks.next().await {
        Span::current().pb_inc(1);
        if let Err(error) = result {
            error!(?error, "task join error");
        }
    }

    Ok(())
}

/// Simulates some work that takes some time and can time out
async fn do_work(n: usize) {
    let total_tasks = ACTIVE.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let metrics = tokio::runtime::Handle::current().metrics();
    let runtime_tasks = metrics.num_alive_tasks();

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        connect_and_run("10.0.0.54:31755", "user", "password", "whoami"),
    )
    .await;

    if n % 10_000 == 0 {
        match result {
            Ok(Ok(output)) => info!(
                n,
                total_tasks,
                runtime_tasks,
                exit_code = output.exit_code,
                stdout = String::from_utf8(output.stdout).unwrap().trim(),
                stderr = String::from_utf8(output.stderr).unwrap().trim(),
                "success"
            ),
            Ok(Err(error)) => error!(n, total_tasks, runtime_tasks, ?error, "failed"),
            Err(_err) => error!(n, total_tasks, runtime_tasks, "timeout"),
        }
    } else {
        match result {
            Ok(Ok(output)) => debug!(
                n,
                total_tasks,
                runtime_tasks,
                exit_code = output.exit_code,
                stdout = String::from_utf8(output.stdout).unwrap().trim(),
                stderr = String::from_utf8(output.stderr).unwrap().trim(),
                "success"
            ),
            Ok(Err(error)) => debug!(n, total_tasks, runtime_tasks, ?error, "failed"),
            Err(_err) => debug!(n, total_tasks, runtime_tasks, "timeout"),
        }
    }
    ACTIVE.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
}

// SSH Client -----------------------------------------------------------------

#[derive(Debug)]
struct RunOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<u32>,
}

async fn connect_and_run(
    target: impl ToSocketAddrs,
    user: &str,
    password: &str,
    command: &str,
) -> Result<RunOutput> {
    let config = russh::client::Config {
        inactivity_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    };
    let config = Arc::new(config);
    let mut session = russh::client::connect(config, target, SshClientHandler).await?;

    session.authenticate_password(user, password).await?;

    let mut channel = session.channel_open_session().await?;
    channel.exec(true, command).await?;

    let mut exit_code = None;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    loop {
        // There's an event available on the session channel
        let Some(msg) = channel.wait().await else {
            break;
        };
        match msg {
            // Copy stdout
            russh::ChannelMsg::Data { ref data } => {
                stdout.write_all(data).await?;
                stdout.flush().await?;
            }
            // Copy stderr
            russh::ChannelMsg::ExtendedData { ref data, ext: 1 } => {
                stderr.write_all(data).await?;
                stderr.flush().await?;
            }
            // The command has returned an exit code
            russh::ChannelMsg::ExitStatus { exit_status } => {
                exit_code = Some(exit_status);
                // cannot leave the loop immediately, there might still be
                // more data to receive
            }
            _ => {}
        }
    }

    Ok(RunOutput {
        stdout,
        stderr,
        exit_code,
    })
}

struct SshClientHandler;

#[async_trait]
impl russh::client::Handler for SshClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh_keys::key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}
