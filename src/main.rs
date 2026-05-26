use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use clap::Parser;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::{task::JoinSet, time::Instant};

use crate::client::{Client, GetRequest, PutRequest, Request, RequestError};
use indicatif::ProgressBar;

mod client;

#[derive(Deserialize)]
struct BenchConfig {
    server: String,
    num_requests: usize,
    concurrency: usize,
    file_size: usize,
}

#[derive(Parser)]
struct Cli {
    /// Path to the YAML configuration file
    #[arg(short, long)]
    config: String,
    /// Path to write the JSON output
    #[arg(short, long)]
    output: String,
}

struct TestConfig {
    host_addr: SocketAddr,
    num_requests: usize,
    file_size: usize,
    concurrency: usize,
}

struct TestCtx {
    host_addr: SocketAddr,
    request: Request,
    requests_remaining: AtomicUsize,
    progress: ProgressBar
}

struct Tester {
    config: TestConfig,
}

#[derive(Serialize)]
struct RequestOutcome {
    duration_ms: Option<u64>,
    error: Option<String>,
}

#[derive(Serialize)]
struct BenchOutput {
    total_elapsed_ms: u64,
    requests: Vec<RequestOutcome>,
}

impl Tester {
    fn new(config: TestConfig) -> Self {
        Self { config }
    }

    async fn setup(&self) -> Result<String, RequestError> {
        let mut rng = rand::rng();

        let mut name_bytes = [0u8; 16];
        rng.fill_bytes(&mut name_bytes);
        let filename: String = name_bytes.iter().map(|b| format!("{:02x}", b)).collect();

        let mut data = vec![0u8; self.config.file_size];
        rng.fill_bytes(&mut data);

        let client = Client::new(self.config.host_addr);
        client.put(&PutRequest { filename: filename.clone(), data }).await?;

        Ok(filename)
    }

    async fn worker(ctx: Arc<TestCtx>) -> Vec<Result<Duration, RequestError>> {
        let mut results = Vec::new();
        loop {
            let claimed = ctx.requests_remaining.fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |x| if x > 0 { Some(x - 1) } else { None },
            );
            if claimed.is_err() {
                break;
            }

            let client = Client::new(ctx.host_addr);
            let before = Instant::now();
            let outcome = match &ctx.request {
                Request::Get(req) => client.get(req).await.map(|_| ()),
                Request::Put(req) => client.put(req).await.map(|_| ()),
                Request::Delete(req) => client.delete(req).await.map(|_| ()),
                Request::List => client.list().await.map(|_| ()),
            };
            let elapsed = before.elapsed();
            results.push(outcome.map(|_| elapsed));
            ctx.progress.inc(1);
        }
        results
    }

    async fn run(&mut self) -> Result<BenchOutput, RequestError> {
        let filename = self.setup().await?;

        let ctx = Arc::new(TestCtx {
            host_addr: self.config.host_addr,
            request: Request::Get(GetRequest { filename }),
            requests_remaining: AtomicUsize::new(self.config.num_requests),
            progress: ProgressBar::new(self.config.num_requests as u64)
        });

        let start = Instant::now();
        let mut set = JoinSet::new();
        for _ in 0..self.config.concurrency {
            set.spawn(Tester::worker(ctx.clone()));
        }
        let worker_results = set.join_all().await;
        let total_elapsed = start.elapsed();

        let requests: Vec<RequestOutcome> = worker_results
            .into_iter()
            .flatten()
            .map(|r| match r {
                Ok(d) => RequestOutcome {
                    duration_ms: Some(d.as_millis() as u64),
                    error: None,
                },
                Err(e) => RequestOutcome {
                    duration_ms: None,
                    error: Some(format!("{:?}", e)),
                },
            })
            .collect();

        Ok(BenchOutput {
            total_elapsed_ms: total_elapsed.as_millis() as u64,
            requests,
        })
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let config_str = std::fs::read_to_string(&cli.config)
        .unwrap_or_else(|e| { eprintln!("Failed to read config: {e}"); std::process::exit(1); });
    let bench_config: BenchConfig = serde_yaml::from_str(&config_str)
        .unwrap_or_else(|e| { eprintln!("Failed to parse YAML: {e}"); std::process::exit(1); });

    let host_addr: SocketAddr = bench_config.server.parse()
        .unwrap_or_else(|e| { eprintln!("Invalid server address: {e}"); std::process::exit(1); });

    let mut tester = Tester::new(TestConfig {
        host_addr,
        num_requests: bench_config.num_requests,
        concurrency: bench_config.concurrency,
        file_size: bench_config.file_size,
    });

    match tester.run().await {
        Ok(output) => {
            println!("Total elapsed: {}ms", output.total_elapsed_ms);
            let json = serde_json::to_string_pretty(&output)
                .expect("Failed to serialize output");
            std::fs::write(&cli.output, &json)
                .unwrap_or_else(|e| { eprintln!("Failed to write output: {e}"); std::process::exit(1); });
            println!("Results written to {}", cli.output);
        }
        Err(e) => {
            eprintln!("Setup failed: {:?}", e);
            std::process::exit(1);
        }
    }
}
