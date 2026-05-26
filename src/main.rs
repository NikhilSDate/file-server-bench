use std::net::SocketAddr;

use clap::Parser;
use serde::Deserialize;

use benchmark::{TestConfig, Tester};

mod benchmark;
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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let config_str = std::fs::read_to_string(&cli.config).unwrap_or_else(|e| {
        eprintln!("Failed to read config: {e}");
        std::process::exit(1);
    });
    let bench_config: BenchConfig = serde_yaml::from_str(&config_str).unwrap_or_else(|e| {
        eprintln!("Failed to parse YAML: {e}");
        std::process::exit(1);
    });

    let host_addr: SocketAddr = bench_config.server.parse().unwrap_or_else(|e| {
        eprintln!("Invalid server address: {e}");
        std::process::exit(1);
    });

    let mut tester = Tester::new(TestConfig {
        host_addr,
        num_requests: bench_config.num_requests,
        concurrency: bench_config.concurrency,
        file_size: bench_config.file_size,
    });

    match tester.run().await {
        Ok(output) => {
            println!("Total elapsed: {}ms", output.total_elapsed_ms);
            let json = serde_json::to_string_pretty(&output).expect("Failed to serialize output");
            std::fs::write(&cli.output, &json).unwrap_or_else(|e| {
                eprintln!("Failed to write output: {e}");
                std::process::exit(1);
            });
            println!("Results written to {}", cli.output);
        }
        Err(e) => {
            eprintln!("Setup failed: {:?}", e);
            std::process::exit(1);
        }
    }
}
