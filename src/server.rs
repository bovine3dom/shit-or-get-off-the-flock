use crate::model::{estimate_wait, now, ordered, Job, Request, Response, State, Status};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct Config {
    pub dir: PathBuf,
    pub lock: PathBuf,
    pub max_shared: usize,
    pub max_runtime: u64,
    pub default_estimate: u64,
    pub startup: u64,
    pub idle_threshold: f64,
    pub idle_seconds: u64,
    pub usage_command: Option<String>,
}

struct Running {
    child: Child,
    _lock: File,
    exclusive: bool,
    started: Instant,
    low_since: Option<Instant>,
    stopping: Option<(Instant, Status, String)>,
    exit: Option<std::process::ExitStatus>,
}

struct Sample {
    child: Child,
    started: Instant,
}

struct Server {
    config: Config,
    state: State,
    running: BTreeMap<u64, Running>,
    draining: Option<u64>,
    sample: Option<Sample>,
    next_sample: Instant,
}

fn private_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
}

fn open_lock(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o666)
        .open(path)
}

fn try_lock(file: &File, exclusive: bool) -> io::Result<bool> {
    let mode = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    if unsafe { libc::flock(file.as_raw_fd(), mode | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::WouldBlock {
        Ok(false)
    } else {
        Err(error)
    }
}

fn signal_group(pid: u32, signal: i32) {
    unsafe {
        libc::kill(-(pid as i32), signal);
    }
}

fn group_exists(pid: u32) -> bool {
    let result = unsafe { libc::kill(-(pid as i32), 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

impl Server {
    fn save(&self) -> io::Result<()> {
        let temp = self.config.dir.join("state.json.tmp");
        let mut file = private_file(&temp)?;
        serde_json::to_writer(&mut file, &self.state)?;
        file.sync_all()?;
        fs::rename(temp, self.config.dir.join("state.json"))?;
        File::open(&self.config.dir)?.sync_all()
    }

    fn external_lock(&self, exclusive: bool) -> io::Result<bool> {
        if self.state.recovery_pending {
            return Ok(true);
        }
        if !self.running.is_empty() {
            return Ok(false);
        }
        let file = open_lock(&self.config.lock)?;
        Ok(!try_lock(&file, exclusive)?)
    }

    fn request(&mut self, request: Request) -> io::Result<Response> {
        match request {
            Request::Submit {
                command,
                cwd,
                env,
                estimate,
                exclusive,
            } => {
                if command.is_empty() || command[0].is_empty() {
                    return Ok(Response::error("command is empty"));
                }
                let estimate = estimate.unwrap_or_else(|| {
                    if self.state.durations.is_empty() {
                        self.config.default_estimate
                    } else {
                        (self.state.durations.iter().copied().sum::<u64>()
                            / self.state.durations.len() as u64)
                            .clamp(1, self.config.max_runtime)
                    }
                });
                if estimate == 0 || estimate > self.config.max_runtime {
                    return Ok(Response::error(
                        "estimate must be between 1 and the maximum runtime",
                    ));
                }
                let id = self.state.next_id.max(1);
                self.state.next_id = id
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("job ID exhausted"))?;
                self.state.jobs.push(Job {
                    id,
                    command,
                    cwd,
                    env,
                    submitted: now(),
                    estimate,
                    exclusive,
                    started: None,
                    finished: None,
                    status: Status::Queued,
                    exit_code: None,
                    detail: None,
                });
                let active: Vec<_> = self
                    .running
                    .iter()
                    .map(|(id, r)| {
                        let job = self.state.jobs.iter().find(|j| j.id == *id).unwrap();
                        (
                            job.estimate.saturating_sub(r.started.elapsed().as_secs()),
                            r.exclusive,
                        )
                    })
                    .collect();
                let needs_exclusive = self.draining.is_some()
                    || ordered(&self.state)
                        .iter()
                        .take_while(|j| j.id != id)
                        .any(|j| j.exclusive)
                    || exclusive;
                let external = self.external_lock(needs_exclusive)?;
                self.save()?;
                let mut response = Response::jobs(vec![self.state.jobs.last().unwrap().clone()]);
                response.wait_seconds = if external {
                    None
                } else {
                    estimate_wait(
                        &self.state,
                        id,
                        &active,
                        self.config.max_shared,
                        self.draining,
                    )
                };
                Ok(response)
            }
            Request::List => Ok(Response::jobs(self.state.jobs.clone())),
            Request::Get { id } => Ok(match self.state.jobs.iter().find(|j| j.id == id) {
                Some(job) => Response::jobs(vec![job.clone()]),
                None => Response::error("job not found"),
            }),
            Request::Cancel { id } => {
                let Some(job) = self.state.jobs.iter_mut().find(|j| j.id == id) else {
                    return Ok(Response::error("job not found"));
                };
                if job.status == Status::Queued {
                    job.status = Status::Cancelled;
                    job.finished = Some(now());
                } else if self.running.contains_key(&id) {
                    self.stop(id, Status::Cancelled, "cancelled by user");
                } else {
                    return Ok(Response::error("job is already finished"));
                }
                self.save()?;
                Ok(Response::jobs(vec![self
                    .state
                    .jobs
                    .iter()
                    .find(|j| j.id == id)
                    .unwrap()
                    .clone()]))
            }
        }
    }

    fn stop(&mut self, id: u64, status: Status, reason: &str) {
        if let Some(running) = self.running.get_mut(&id) {
            if running.stopping.is_none() {
                signal_group(running.child.id(), libc::SIGTERM);
                running.stopping = Some((Instant::now(), status, reason.to_owned()));
                let job = self.state.jobs.iter_mut().find(|j| j.id == id).unwrap();
                job.status = Status::Stopping;
                job.detail = Some(reason.to_owned());
            }
        }
    }

    fn start_next(&mut self) -> io::Result<()> {
        if self.state.recovery_pending {
            if !self.running.is_empty() {
                return Ok(());
            }
            let lock = open_lock(&self.config.lock)?;
            if !try_lock(&lock, true)? {
                return Ok(());
            }
            self.state.recovery_pending = false;
            self.save()?;
        }
        if self.draining.is_some_and(|id| {
            !self
                .state
                .jobs
                .iter()
                .any(|j| j.id == id && j.status == Status::Queued)
        }) {
            self.draining = None;
        }
        let next = self
            .draining
            .and_then(|id| self.state.jobs.iter().find(|j| j.id == id))
            .or_else(|| ordered(&self.state).first().copied());
        let Some(job) = next else {
            return Ok(());
        };
        let (id, exclusive) = (job.id, job.exclusive);
        if exclusive && !self.running.is_empty() {
            if self.running.values().all(|r| !r.exclusive) {
                self.draining = Some(id);
            }
            return Ok(());
        }
        if !exclusive
            && (self.running.len() >= self.config.max_shared
                || self.running.values().any(|r| r.exclusive))
        {
            return Ok(());
        }
        let lock = open_lock(&self.config.lock)?;
        if !try_lock(&lock, exclusive)? {
            return Ok(());
        }
        if self.draining == Some(id) {
            self.draining = None;
        }
        let job = self.state.jobs.iter_mut().find(|j| j.id == id).unwrap();
        job.status = Status::Starting;
        job.started = Some(now());
        self.save()?;
        let job = self.state.jobs.iter().find(|j| j.id == id).unwrap();
        let stdout = private_file(&self.config.dir.join(format!("{id}.stdout")))?;
        let stderr = private_file(&self.config.dir.join(format!("{id}.stderr")))?;
        // A non-CLOEXEC duplicate lets the job retain the lock if the scheduler dies.
        let fd = unsafe { libc::fcntl(lock.as_raw_fd(), libc::F_DUPFD, 3) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let inherited = unsafe { File::from_raw_fd(fd) };
        let result = Command::new(&job.command[0])
            .args(&job.command[1..])
            .current_dir(&job.cwd)
            .env_clear()
            .envs(job.env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .process_group(0)
            .spawn();
        drop(inherited);
        match result {
            Ok(child) => {
                self.state
                    .jobs
                    .iter_mut()
                    .find(|j| j.id == id)
                    .unwrap()
                    .status = Status::Running;
                self.running.insert(
                    id,
                    Running {
                        child,
                        _lock: lock,
                        exclusive,
                        started: Instant::now(),
                        low_since: None,
                        stopping: None,
                        exit: None,
                    },
                );
                if exclusive {
                    self.next_sample = Instant::now() + Duration::from_secs(self.config.startup);
                }
            }
            Err(error) => {
                let job = self.state.jobs.iter_mut().find(|j| j.id == id).unwrap();
                job.status = Status::Failed;
                job.finished = Some(now());
                job.detail = Some(format!("failed to start: {error}"));
            }
        }
        self.save()
    }

    fn update_running(&mut self) -> io::Result<()> {
        for id in self.running.keys().copied().collect::<Vec<_>>() {
            let running = self.running.get_mut(&id).unwrap();
            if running.exit.is_none() {
                running.exit = running.child.try_wait()?;
            }
            if running.stopping.is_none() {
                if running.exit.is_some() {
                    self.stop(id, Status::Succeeded, "process exited");
                } else {
                    let estimate = self
                        .state
                        .jobs
                        .iter()
                        .find(|j| j.id == id)
                        .unwrap()
                        .estimate;
                    let limit = self.config.max_runtime.min(estimate.saturating_mul(3));
                    if running.started.elapsed() >= Duration::from_secs(limit) {
                        self.stop(id, Status::Evicted, "runtime limit exceeded");
                    }
                }
            }
            let running = self.running.get_mut(&id).unwrap();
            let Some((since, _, _)) = &running.stopping else {
                continue;
            };
            let group_alive = group_exists(running.child.id());
            if since.elapsed() >= Duration::from_secs(2) {
                signal_group(running.child.id(), libc::SIGKILL);
            }
            if running.exit.is_none() || (group_alive && since.elapsed() < Duration::from_secs(2)) {
                continue;
            }
            let running = self.running.remove(&id).unwrap();
            if group_alive {
                self.state.recovery_pending = true;
            }
            let (_, requested, detail) = running.stopping.unwrap();
            let exit = running.exit.unwrap();
            let job = self.state.jobs.iter_mut().find(|j| j.id == id).unwrap();
            job.status = if requested == Status::Succeeded && !exit.success() {
                Status::Failed
            } else {
                requested
            };
            job.exit_code = exit.code();
            job.detail = if detail == "process exited" {
                None
            } else {
                Some(detail)
            };
            job.finished = Some(now());
            if matches!(job.status, Status::Succeeded | Status::Failed) {
                self.state
                    .durations
                    .push(running.started.elapsed().as_secs().max(1));
                if self.state.durations.len() > 10 {
                    self.state.durations.remove(0);
                }
            }
            // Descendants retain the lock if they outlive the scheduler.
            drop(running._lock);
            if running.exclusive {
                if let Some(mut sample) = self.sample.take() {
                    signal_group(sample.child.id(), libc::SIGKILL);
                    let _ = sample.child.wait();
                }
            }
            self.save()?;
        }
        Ok(())
    }

    fn update_sample(&mut self) -> io::Result<()> {
        if self.config.idle_threshold == 0.0 {
            return Ok(());
        }
        let Some(id) = self
            .running
            .iter()
            .find(|(_, r)| r.exclusive)
            .map(|(id, _)| *id)
        else {
            return Ok(());
        };
        if let Some(sample) = &mut self.sample {
            let status = sample.child.try_wait()?;
            if status.is_none() && sample.started.elapsed() < Duration::from_secs(10) {
                return Ok(());
            }
            if status.is_none() {
                signal_group(sample.child.id(), libc::SIGKILL);
                let _ = sample.child.wait();
            }
            let valid = status.is_some_and(|s| s.success());
            self.sample = None;
            let value = if valid {
                fs::read_to_string(self.config.dir.join("usage.tmp"))
                    .ok()
                    .and_then(|s| s.trim().parse::<f64>().ok())
                    .filter(|v| v.is_finite() && (0.0..=100.0).contains(v))
            } else {
                None
            };
            let running = self.running.get_mut(&id).unwrap();
            if let Some(value) = value {
                if value < self.config.idle_threshold {
                    let since = running.low_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= Duration::from_secs(self.config.idle_seconds) {
                        self.stop(id, Status::Evicted, "device utilization remained low");
                    }
                } else {
                    running.low_since = None;
                }
            } else {
                running.low_since = None;
                eprintln!("usage command failed or returned an invalid percentage");
            }
            self.next_sample = Instant::now() + Duration::from_secs(1);
        } else if self.running.get(&id).unwrap().stopping.is_none()
            && Instant::now() >= self.next_sample
        {
            let output = private_file(&self.config.dir.join("usage.tmp"))?;
            let child = Command::new("/bin/sh")
                .arg("-c")
                .arg(self.config.usage_command.as_ref().unwrap())
                .stdin(Stdio::null())
                .stdout(output)
                .stderr(Stdio::null())
                .process_group(0)
                .spawn()?;
            self.sample = Some(Sample {
                child,
                started: Instant::now(),
            });
        }
        Ok(())
    }
}

fn handle_connection(server: &mut Server, mut stream: UnixStream) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    let mut line = String::new();
    if let Err(error) = BufReader::new(stream.try_clone()?)
        .take(4 * 1024 * 1024)
        .read_line(&mut line)
    {
        eprintln!("request read failed: {error}");
        return Ok(());
    }
    let response = match serde_json::from_str(&line) {
        Ok(request) => server.request(request)?,
        Err(error) => Response::error(format!("invalid request: {error}")),
    };
    let result = serde_json::to_writer(&mut stream, &response)
        .map_err(io::Error::other)
        .and_then(|_| stream.write_all(b"\n"));
    if let Err(error) = result {
        eprintln!("response write failed: {error}");
    }
    Ok(())
}

pub fn serve(config: Config) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&config.dir)?;
    let metadata = fs::metadata(&config.dir)?;
    if metadata.permissions().mode() & 0o077 != 0 || metadata.uid() != unsafe { libc::geteuid() } {
        return Err(io::Error::other(
            "state directory must be owned by the current user and private (mode 0700)",
        ));
    }
    let daemon_lock = open_lock(&config.dir.join("daemon.lock"))?;
    if !try_lock(&daemon_lock, true)? {
        return Err(io::Error::other("scheduler is already running"));
    }
    let state_path = config.dir.join("state.json");
    let mut state: State = if state_path.exists() {
        serde_json::from_reader(File::open(state_path)?)?
    } else {
        State::default()
    };
    for job in &mut state.jobs {
        if matches!(
            job.status,
            Status::Starting | Status::Running | Status::Stopping
        ) {
            state.recovery_pending = true;
            job.status = Status::Interrupted;
            job.finished = Some(now());
            job.detail =
                Some("scheduler stopped; the process may still hold the device lock".into());
        }
    }
    let socket = config.dir.join("scheduler.sock");
    if socket.exists() {
        fs::remove_file(&socket)?;
    }
    let listener = UnixListener::bind(&socket)?;
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    let mut server = Server {
        config,
        state,
        running: BTreeMap::new(),
        draining: None,
        sample: None,
        next_sample: Instant::now(),
    };
    server.save()?;
    eprintln!("scheduler listening on {}", socket.display());
    loop {
        match listener.accept() {
            Ok((stream, _)) => handle_connection(&mut server, stream)?,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        server.update_running()?;
        server.update_sample()?;
        server.start_next()?;
        thread::sleep(Duration::from_millis(100));
    }
}

use std::io::Read;
use std::os::unix::fs::MetadataExt;
