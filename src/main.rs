#[cfg(not(unix))]
compile_error!("sogotf currently supports Unix systems only");

mod model;
mod scheduler;
mod server;

use model::{Access, Request, Response, Status};
use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::Duration;

const HELP: &str = "sogotf [--state-dir DIR] serve --lock PATH [options]
sogotf [--state-dir DIR] [--wait] [--exclusive | [--cpu MODE] [--gpu MODE]] [--threads N] [--approx-execution-time SECONDS] -- COMMAND [ARGS...]
sogotf [--state-dir DIR] list | status ID | cancel ID | drain [--wait] | resume

--cpu and --gpu accept shared or exclusive. With no resource options, jobs
share the GPU; --exclusive requests exclusive CPU and GPU.
--threads N reserves an exact CPU thread count. Use it as a last resort for
fixed-thread programs; prefer SOGOTF_THREADS when possible.

Server options:
  --cpu-lock PATH             CPU flock file (default: STATE_DIR/cpu.lock)
  --cpu-threads THREADS       Available CPU threads (default: detected CPUs)
  --batch-ms MILLISECONDS     CPU shared-job batching window (default: 1000)
  --max-shared JOBS           Maximum concurrent shared GPU jobs (default: 10)
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

fn access(value: &str) -> Result<Access, String> {
    match value {
        "shared" => Ok(Access::Shared),
        "exclusive" => Ok(Access::Exclusive),
        _ => Err(format!(
            "invalid resource mode: {value}; use shared or exclusive"
        )),
    }
}

fn request(dir: &Path, request: &Request) -> io::Result<Response> {
    let mut stream = UnixStream::connect(dir.join("scheduler.sock"))?;
    {
        let mut writer = io::BufWriter::new(&mut stream);
        serde_json::to_writer(&mut writer, request)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    }
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    if !line.ends_with('\n') {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "scheduler closed connection",
        ));
    }
    let response: Response = serde_json::from_str(&line)?;
    if let Some(error) = &response.error {
        return Err(io::Error::other(error.clone()));
    }
    Ok(response)
}

fn retry_request(dir: &Path, req: &Request) -> io::Result<Response> {
    loop {
        match request(dir, req) {
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound
                        | io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::UnexpectedEof
                ) =>
            {
                thread::sleep(Duration::from_millis(250));
            }
            result => return result,
        }
    }
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
            cpu_lock: dir.join("cpu.lock"),
            dir,
            lock: PathBuf::new(),
            cpu_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            batch_ms: 1000,
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
                "--cpu-lock" => config.cpu_lock = PathBuf::from(value(rest, &mut i)?),
                "--cpu-threads" => {
                    config.cpu_threads = number(&value(rest, &mut i)?, "CPU threads")?
                }
                "--batch-ms" => config.batch_ms = number(&value(rest, &mut i)?, "batch window")?,
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
        if config.cpu_threads == 0 {
            return Err("CPU threads must be positive".into());
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
            let response = request(&dir, &Request::List).map_err(|e| e.to_string())?;
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
            let response = request(&dir, &req).map_err(|e| e.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&response.jobs[0]).map_err(|e| e.to_string())?
            );
            return Ok(0);
        }
        "drain" if rest.len() == 1 || (rest.len() == 2 && rest[1] == "--wait") => {
            let mut response = request(&dir, &Request::Drain).map_err(|e| e.to_string())?;
            if rest.len() == 2 {
                while response.drained != Some(true) {
                    thread::sleep(Duration::from_millis(250));
                    response = retry_request(&dir, &Request::Drain).map_err(|e| e.to_string())?;
                }
            }
            println!(
                "{}",
                if response.drained == Some(true) {
                    "scheduler drained"
                } else {
                    "scheduler draining"
                }
            );
            return Ok(0);
        }
        "resume" if rest.len() == 1 => {
            request(&dir, &Request::Resume).map_err(|e| e.to_string())?;
            println!("scheduler resumed");
            return Ok(0);
        }
        _ => {}
    }
    let mut wait = false;
    let mut exclusive = false;
    let mut gpu = None;
    let mut cpu = None;
    let mut requested_threads = None;
    let mut estimate = None;
    let mut i = 0;
    while i < rest.len() {
        match rest[i].as_str() {
            "--wait" => wait = true,
            "--exclusive" => exclusive = true,
            "--cpu" => cpu = Some(access(&value(rest, &mut i)?)?),
            "--gpu" => gpu = Some(access(&value(rest, &mut i)?)?),
            "--threads" => requested_threads = Some(number(&value(rest, &mut i)?, "thread count")?),
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
    if exclusive && (cpu.is_some() || gpu.is_some()) {
        return Err("--exclusive cannot be combined with --cpu or --gpu".into());
    }
    if exclusive {
        cpu = Some(Access::Exclusive);
        gpu = Some(Access::Exclusive);
    }
    if cpu.is_none() && gpu.is_none() {
        gpu = Some(Access::Shared);
    }
    if requested_threads.is_some() && cpu.is_none() {
        return Err("--threads requires --cpu or --exclusive".into());
    }
    if requested_threads == Some(0) {
        return Err("thread count must be positive".into());
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
            exclusive: gpu == Some(Access::Exclusive),
            gpu: gpu.is_some(),
            cpu,
            requested_threads,
            resources_explicit: true,
        },
    )
    .map_err(|e| e.to_string())?;
    let id = response.jobs[0].id;
    match response.wait_seconds {
        Some(seconds) => println!("job {id}: estimated time until start: {seconds}s"),
        None => println!(
            "job {id}: estimated time until start: unknown (resource availability, recovery or pause)"
        ),
    }
    println!(
        "stdout: {}\nstderr: {}",
        dir.join(format!("{id}.stdout")).display(),
        dir.join(format!("{id}.stderr")).display()
    );
    if wait {
        loop {
            let response = retry_request(&dir, &Request::Get { id }).map_err(|e| e.to_string())?;
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
