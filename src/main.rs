#[cfg(not(unix))]
compile_error!("sogotf currently supports Unix systems only");

mod model;
mod server;

use model::{Request, Response, Status};
use std::env;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::Duration;

const HELP: &str = "sogotf [--state-dir DIR] serve --lock PATH [options]
sogotf [--state-dir DIR] [--wait] [--exclusive] [--approx-execution-time SECONDS] -- COMMAND [ARGS...]
sogotf [--state-dir DIR] list | status ID | cancel ID

Jobs share the device by default; --exclusive requires sole access.

Server options:
  --max-shared JOBS           Maximum concurrent shared jobs (default: 10)
  --max-runtime SECONDS       Hard runtime limit (default: 86400)
  --default-estimate SECONDS  Estimate without history (default: 3600)
  --startup SECONDS           Idle-check grace period (default: 60)
  --idle-threshold PERCENT    Evict below this utilization; 0 disables (default: 0)
  --idle-seconds SECONDS      Required low-utilization time (default: 60)
  --usage-command COMMAND     Shell command that prints one percentage

State directory: --state-dir, SOGOTF_DIR, or ~/.local/state/sogotf.";

fn value(args: &[String], i: &mut usize) -> Result<String, String> {
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| format!("{} requires a value", args[*i - 1]))
}

fn number<T: std::str::FromStr>(s: &str, name: &str) -> Result<T, String> {
    s.parse().map_err(|_| format!("invalid {name}: {s}"))
}

fn request(dir: &Path, request: &Request) -> Result<Response, String> {
    let mut stream = UnixStream::connect(dir.join("scheduler.sock"))
        .map_err(|e| format!("connect to scheduler: {e}"))?;
    serde_json::to_writer(&mut stream, request).map_err(|e| e.to_string())?;
    stream.write_all(b"\n").map_err(|e| e.to_string())?;
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .map_err(|e| e.to_string())?;
    let response: Response =
        serde_json::from_str(&line).map_err(|e| format!("invalid scheduler response: {e}"))?;
    if let Some(error) = &response.error {
        return Err(error.clone());
    }
    Ok(response)
}

fn run() -> Result<i32, String> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0;
    let mut dir = env::var_os("SOGOTF_DIR")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|p| PathBuf::from(p).join(".local/state/sogotf")));
    if args.first().is_some_and(|a| a == "--state-dir") {
        dir = Some(PathBuf::from(value(&args, &mut i)?));
        i += 1;
    }
    let dir = dir.ok_or("set --state-dir or SOGOTF_DIR")?;
    let rest = &args[i..];
    if rest.is_empty() || rest[0] == "--help" || rest[0] == "-h" {
        println!("{HELP}");
        return Ok(0);
    }
    if rest[0] == "serve" {
        let mut config = server::Config {
            dir,
            lock: PathBuf::new(),
            max_shared: 10,
            max_runtime: 86400,
            default_estimate: 3600,
            startup: 60,
            idle_threshold: 0.0,
            idle_seconds: 60,
            usage_command: None,
        };
        let mut i = 1;
        while i < rest.len() {
            match rest[i].as_str() {
                "--lock" => config.lock = PathBuf::from(value(rest, &mut i)?),
                "--max-shared" => {
                    config.max_shared = number(&value(rest, &mut i)?, "max shared jobs")?
                }
                "--max-runtime" => {
                    config.max_runtime = number(&value(rest, &mut i)?, "max runtime")?
                }
                "--default-estimate" => {
                    config.default_estimate = number(&value(rest, &mut i)?, "default estimate")?
                }
                "--startup" => config.startup = number(&value(rest, &mut i)?, "startup")?,
                "--idle-threshold" => {
                    config.idle_threshold = number(&value(rest, &mut i)?, "idle threshold")?
                }
                "--idle-seconds" => {
                    config.idle_seconds = number(&value(rest, &mut i)?, "idle seconds")?
                }
                "--usage-command" => config.usage_command = Some(value(rest, &mut i)?),
                other => return Err(format!("unknown server option: {other}")),
            }
            i += 1;
        }
        if config.lock.as_os_str().is_empty() {
            return Err("serve requires --lock PATH".into());
        }
        if config.max_shared == 0 {
            return Err("max shared jobs must be positive".into());
        }
        if config.max_runtime == 0
            || config.default_estimate == 0
            || config.default_estimate > config.max_runtime
        {
            return Err("default estimate and maximum runtime must be positive, and default must not exceed maximum".into());
        }
        if !config.idle_threshold.is_finite() || !(0.0..=100.0).contains(&config.idle_threshold) {
            return Err("idle threshold must be between 0 and 100".into());
        }
        if config.idle_threshold > 0.0
            && (config.usage_command.is_none() || config.idle_seconds == 0)
        {
            return Err(
                "idle eviction requires --usage-command and a positive --idle-seconds".into(),
            );
        }
        server::serve(config).map_err(|e| e.to_string())?;
        return Ok(0);
    }
    match rest[0].as_str() {
        "list" if rest.len() == 1 => {
            let response = request(&dir, &Request::List)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&response.jobs).map_err(|e| e.to_string())?
            );
            return Ok(0);
        }
        "status" | "cancel" if rest.len() == 2 => {
            let id = number(&rest[1], "job ID")?;
            let req = if rest[0] == "status" {
                Request::Get { id }
            } else {
                Request::Cancel { id }
            };
            let response = request(&dir, &req)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&response.jobs[0]).map_err(|e| e.to_string())?
            );
            return Ok(0);
        }
        _ => {}
    }
    let mut wait = false;
    let mut exclusive = false;
    let mut estimate = None;
    let mut i = 0;
    while i < rest.len() {
        match rest[i].as_str() {
            "--wait" => wait = true,
            "--exclusive" => exclusive = true,
            "--approx-execution-time" => {
                estimate = Some(number(&value(rest, &mut i)?, "estimate")?)
            }
            "--" => {
                i += 1;
                break;
            }
            option if option.starts_with('-') => return Err(format!("unknown option: {option}")),
            _ => break,
        }
        i += 1;
    }
    if i >= rest.len() {
        return Err("a command is required".into());
    }
    let cwd = env::current_dir()
        .map_err(|e| e.to_string())?
        .into_os_string()
        .into_string()
        .map_err(|_| "working directory is not UTF-8")?;
    let environment = env::vars_os()
        .map(|(k, v)| {
            Ok((
                k.into_string()
                    .map_err(|_| "environment key is not UTF-8")?,
                v.into_string()
                    .map_err(|_| "environment value is not UTF-8")?,
            ))
        })
        .collect::<Result<Vec<_>, &str>>()?;
    let response = request(
        &dir,
        &Request::Submit {
            command: rest[i..].to_vec(),
            cwd,
            env: environment,
            estimate,
            exclusive,
        },
    )?;
    let id = response.jobs[0].id;
    match response.wait_seconds {
        Some(seconds) => println!("job {id}: estimated time until start: {seconds}s"),
        None => println!("job {id}: estimated time until start: unknown (external lock held or recovery pending)"),
    }
    println!(
        "stdout: {}\nstderr: {}",
        dir.join(format!("{id}.stdout")).display(),
        dir.join(format!("{id}.stderr")).display()
    );
    if wait {
        loop {
            let response = request(&dir, &Request::Get { id })?;
            let job = &response.jobs[0];
            if job.status.terminal() {
                println!(
                    "job {id}: {:?}{}",
                    job.status,
                    job.detail
                        .as_ref()
                        .map(|d| format!(" ({d})"))
                        .unwrap_or_default()
                );
                return Ok(if job.status == Status::Succeeded {
                    0
                } else {
                    job.exit_code
                        .filter(|&n| (1..=255).contains(&n))
                        .unwrap_or(1)
                });
            }
            thread::sleep(Duration::from_millis(250));
        }
    }
    Ok(0)
}

fn main() {
    match run() {
        Ok(code) => process::exit(code),
        Err(error) => {
            eprintln!("sogotf: {error}");
            process::exit(1);
        }
    }
}
