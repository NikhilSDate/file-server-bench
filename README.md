# File Server Benchmark

A configurable HTTP file-server benchmark tool for the protocol implemented in UIUC CS341.

## Usage

```
cargo run -- --config <config.yaml> --output <results.json>
```

**Example:**

```
cargo run -- --config ./configs/simple.yaml --output ./results.json
```

## Configuration

Config files are YAML with four fields:

```yaml
server: "127.0.0.1:9001"   # server IP and port
num_requests: 10            # total number of GET requests to send
concurrency: 1              # number of concurrent connections
file_size: 1024             # size of the file to upload during setup (bytes)
```

Pre-made configs are in [configs/](configs/):

| Config | Requests | Concurrency | File size |
|--------|----------|-------------|-----------|
| `simple.yaml` | 10 | 1 | 1 KB |

## Output

Results are written as JSON:

```json
{
  "total_elapsed_ms": 42,
  "requests": [
    { "duration_ms": 4, "error": null },
    { "duration_ms": null, "error": "IoError(...)" }
  ]
}
```

`total_elapsed_ms` is the wall-clock time for the entire run. Each entry in `requests` has either a `duration_ms` for successful requests or an `error` string for failures.
