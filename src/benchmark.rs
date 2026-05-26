use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use indicatif::ProgressBar;
use rand::Rng;
use serde::Serialize;
use tokio::{task::JoinSet, time::Instant};

use crate::client::{Client, GetRequest, PutRequest, Request, RequestError};

pub struct TestConfig {
    pub host_addr: SocketAddr,
    pub num_requests: usize,
    pub file_size: usize,
    pub concurrency: usize,
}

struct RequestCounter(AtomicUsize)

impl RequestCounter {
    fn claim(&self) -> Option<usize> {
        let claimed =
            self.0
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                    if x > 0 { Some(x - 1) } else { None }
                });
        claimed.ok()
    }
}

struct TestCtx {
    host_addr: SocketAddr,
    request: Request,
    counter: RequestCounter,
    progress: ProgressBar,
}

impl TestCtx {
    // tries to claim a request
    // returns remaining requests BEFORE request was claimed

}

pub struct Tester {
    config: TestConfig,
}

#[derive(Serialize)]
pub struct RequestOutcome {
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct BenchOutput {
    pub total_elapsed_ms: u64,
    pub requests: Vec<RequestOutcome>,
}

impl Tester {
    pub fn new(config: TestConfig) -> Self {
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
        client
            .put(&PutRequest {
                filename: filename.clone(),
                data,
            })
            .await?;

        Ok(filename)
    }

    async fn worker(ctx: Arc<TestCtx>) -> Vec<Result<Duration, RequestError>> {
        let mut results = Vec::new();
        loop {
            if ctx.counter.claim().is_none() {
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

    pub async fn run(&mut self) -> Result<BenchOutput, RequestError> {
        let filename = self.setup().await?;

        let ctx = Arc::new(TestCtx {
            host_addr: self.config.host_addr,
            request: Request::Get(GetRequest { filename }),
            requests_remaining: AtomicUsize::new(self.config.num_requests),
            progress: ProgressBar::new(self.config.num_requests as u64),
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
