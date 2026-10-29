use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Harness {
    dir: PathBuf,
    child: Child,
    extra: Vec<String>,
}

fn estimated_wait(output: &Output) -> Option<u64> {
    let text = String::from_utf8_lossy(&output.stdout);
    let value = text
        .lines()
        .next()?
        .split("estimated time until start: ")
        .nth(1)?;
    value.strip_suffix('s')?.parse().ok()
}
impl Harness {
    fn new(extra: &[&str]) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "sogotf-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        let lock = dir.join("device.lock");
        let child = Command::new(env!("CARGO_BIN_EXE_sogotf"))
            .arg("--state-dir")
            .arg(&dir)
            .args(["serve", "--lock"])
            .arg(&lock)
            .args(["--max-runtime", "20", "--default-estimate", "5"])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let harness = Self {
            dir,
            child,
            extra: extra.iter().map(|s| (*s).to_owned()).collect(),
        };
        let until = Instant::now() + Duration::from_secs(5);
        while !harness.dir.join("scheduler.sock").exists() {
            assert!(Instant::now() < until, "scheduler failed to start");
            sleep(Duration::from_millis(20));
        }
        harness
    }
    fn restart(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.start();
    }
    fn start(&mut self) {
        self.child = Command::new(env!("CARGO_BIN_EXE_sogotf"))
            .arg("--state-dir")
            .arg(&self.dir)
            .args(["serve", "--lock"])
            .arg(self.dir.join("device.lock"))
            .args(["--max-runtime", "20", "--default-estimate", "5"])
            .args(&self.extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            if Command::new(env!("CARGO_BIN_EXE_sogotf"))
                .arg("--state-dir")
                .arg(&self.dir)
                .arg("list")
                .output()
                .unwrap()
                .status
                .success()
            {
                break;
            }
            assert!(Instant::now() < until, "scheduler failed to restart");
            sleep(Duration::from_millis(50));
        }
    }
    fn invoke(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_sogotf"))
            .arg("--state-dir")
            .arg(&self.dir)
            .args(args)
            .output()
            .unwrap()
    }
    fn run(&self, args: &[&str]) -> Output {
        let output = self.invoke(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
    fn job(&self, id: u64) -> Value {
        serde_json::from_slice(&self.run(&["status", &id.to_string()]).stdout).unwrap()
    }
    fn wait_status(&self, id: u64, wanted: &[&str]) -> Value {
        let until = Instant::now() + Duration::from_secs(12);
        loop {
            let job = self.job(id);
            if wanted.contains(&job["status"].as_str().unwrap()) {
                return job;
            }
            assert!(Instant::now() < until, "job {id} stuck: {job}");
            sleep(Duration::from_millis(100));
        }
    }
    fn lock(&self) -> File {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.dir.join("device.lock"))
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        f
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn external_flock_and_deadline_order() {
    let h = Harness::new(&[]);
    let held = h.lock();
    let path = h.dir.join("order");
    let long = format!("echo long >> {}", path.display());
    let short = format!("echo short >> {}", path.display());
    let first = h.run(&[
        "--exclusive",
        "--approx-execution-time",
        "10",
        "--",
        "/bin/sh",
        "-c",
        &long,
    ]);
    assert!(String::from_utf8_lossy(&first.stdout).contains("unknown"));
    h.run(&[
        "--exclusive",
        "--approx-execution-time",
        "1",
        "--",
        "/bin/sh",
        "-c",
        &short,
    ]);
    assert_eq!(h.job(1)["status"], "queued");
    drop(held);
    h.wait_status(1, &["succeeded"]);
    h.wait_status(2, &["succeeded"]);
    assert_eq!(fs::read_to_string(path).unwrap(), "short\nlong\n");
}

#[test]
fn cancellation_and_runtime_eviction() {
    let h = Harness::new(&[]);
    h.run(&[
        "--exclusive",
        "--approx-execution-time",
        "1",
        "--",
        "/bin/sleep",
        "20",
    ]);
    h.wait_status(1, &["running"]);
    h.run(&["--exclusive", "--", "/bin/sleep", "20"]);
    h.run(&["cancel", "2"]);
    assert_eq!(h.job(2)["status"], "cancelled");
    let job = h.wait_status(1, &["evicted"]);
    assert_eq!(job["detail"], "runtime limit exceeded");
}

#[test]
fn idle_eviction() {
    let h = Harness::new(&[
        "--startup",
        "0",
        "--idle-threshold",
        "10",
        "--idle-seconds",
        "1",
        "--usage-command",
        "echo 0",
    ]);
    h.run(&["--exclusive", "--", "/bin/sleep", "20"]);
    let job = h.wait_status(1, &["evicted"]);
    assert_eq!(job["detail"], "device utilization remained low");
}

#[test]
fn queued_job_survives_restart() {
    let mut h = Harness::new(&[]);
    let held = h.lock();
    h.run(&["--", "/bin/true"]);
    h.restart();
    assert_eq!(h.job(1)["status"], "queued");
    drop(held);
    h.wait_status(1, &["succeeded"]);
}

#[test]
fn shared_jobs_obey_capacity_and_cancel_independently() {
    let h = Harness::new(&["--max-shared", "2"]);
    for _ in 0..3 {
        h.run(&["--", "/bin/sleep", "8"]);
    }
    h.wait_status(1, &["running"]);
    h.wait_status(2, &["running"]);
    assert_eq!(h.job(1)["exclusive"], false);
    assert_eq!(h.job(3)["status"], "queued");
    let probe = OpenOptions::new()
        .read(true)
        .write(true)
        .open(h.dir.join("device.lock"))
        .unwrap();
    assert_ne!(
        unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    assert_eq!(
        unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) },
        0
    );
    drop(probe);
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
    h.wait_status(3, &["running"]);
    assert_eq!(h.job(2)["status"], "running");
    h.run(&["cancel", "2"]);
    h.run(&["cancel", "3"]);
    h.wait_status(2, &["cancelled"]);
    h.wait_status(3, &["cancelled"]);
}

#[test]
fn exclusive_drains_shared_jobs_before_later_arrivals() {
    let h = Harness::new(&["--max-shared", "2"]);
    h.run(&["--approx-execution-time", "10", "--", "/bin/sleep", "5"]);
    h.wait_status(1, &["running"]);
    h.run(&[
        "--exclusive",
        "--approx-execution-time",
        "2",
        "--",
        "/bin/sleep",
        "1",
    ]);
    h.run(&["--approx-execution-time", "1", "--", "/bin/sleep", "1"]);
    assert_eq!(h.job(2)["status"], "queued");
    assert_eq!(h.job(3)["status"], "queued");
    h.wait_status(2, &["running"]);
    assert_eq!(h.job(1)["status"], "succeeded");
    assert_eq!(h.job(3)["status"], "queued");
    let exclusive = h.wait_status(2, &["succeeded"]);
    let shared = h.wait_status(3, &["succeeded"]);
    assert!(shared["started"].as_u64() >= exclusive["finished"].as_u64());
}

#[test]
fn cancelling_exclusive_job_reopens_shared_slots() {
    let h = Harness::new(&["--max-shared", "2"]);
    h.run(&["--", "/bin/sleep", "8"]);
    h.wait_status(1, &["running"]);
    h.run(&[
        "--exclusive",
        "--approx-execution-time",
        "1",
        "--",
        "/bin/sleep",
        "8",
    ]);
    h.run(&["cancel", "2"]);
    h.run(&["--", "/bin/sleep", "8"]);
    h.wait_status(3, &["running"]);
    assert_eq!(h.job(1)["status"], "running");
    h.run(&["cancel", "1"]);
    h.run(&["cancel", "3"]);
    h.wait_status(1, &["cancelled"]);
    h.wait_status(3, &["cancelled"]);
}

#[test]
fn shared_jobs_skip_idle_eviction() {
    let h = Harness::new(&[
        "--startup",
        "0",
        "--idle-threshold",
        "10",
        "--idle-seconds",
        "1",
        "--usage-command",
        "echo 0",
    ]);
    h.run(&["--", "/bin/sleep", "4"]);
    assert_eq!(h.wait_status(1, &["succeeded"])["exclusive"], false);
}

#[test]
fn external_shared_lock_cooperates_with_both_modes() {
    let h = Harness::new(&[]);
    let held = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(h.dir.join("device.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) },
        0
    );
    h.run(&["--", "/bin/sleep", "1"]);
    h.wait_status(1, &["succeeded"]);
    let response = h.run(&["--exclusive", "--", "/bin/true"]);
    assert!(String::from_utf8_lossy(&response.stdout).contains("unknown"));
    assert_eq!(h.job(2)["status"], "queued");
    drop(held);
    h.wait_status(2, &["succeeded"]);
}

#[test]
fn recovery_waits_for_orphaned_shared_jobs_across_restarts() {
    let mut h = Harness::new(&[]);
    h.run(&["--", "/bin/sleep", "6"]);
    h.wait_status(1, &["running"]);
    h.restart();
    assert_eq!(h.job(1)["status"], "interrupted");
    let result = h.run(&["--", "/bin/true"]);
    assert!(String::from_utf8_lossy(&result.stdout).contains("unknown"));
    assert_eq!(h.job(2)["status"], "queued");
    let draining = h.run(&["drain"]);
    assert!(String::from_utf8_lossy(&draining.stdout).contains("draining"));
    h.restart();
    assert_eq!(h.job(2)["status"], "queued");
    h.run(&["drain", "--wait"]);
    assert_eq!(h.job(2)["status"], "queued");
    h.run(&["resume"]);
    h.wait_status(2, &["succeeded"]);
}

#[test]
fn cpu_batch_submission_returns_estimates() {
    let h = Harness::new(&["--cpu-threads", "60", "--batch-ms", "2000"]);
    let first = h.run(&["--cpu", "shared", "--", "/bin/sleep", "6"]);
    let second = h.run(&["--cpu", "shared", "--", "/bin/sleep", "6"]);
    assert_eq!(estimated_wait(&first), Some(2));
    assert_eq!(estimated_wait(&second), Some(2));
    assert_eq!(h.wait_status(1, &["running"])["threads"], 30);
    assert_eq!(h.wait_status(2, &["running"])["threads"], 30);
    h.run(&["cancel", "1"]);
    h.run(&["cancel", "2"]);
    h.wait_status(1, &["cancelled"]);
    h.wait_status(2, &["cancelled"]);
}

#[test]
fn drain_preserves_queued_jobs_and_wait_clients_across_restart() {
    let mut h = Harness::new(&["--cpu-threads", "1", "--batch-ms", "0"]);
    h.run(&["--cpu", "shared", "--threads", "1", "--", "/bin/sleep", "3"]);
    h.wait_status(1, &["running"]);
    let waiter = Command::new(env!("CARGO_BIN_EXE_sogotf"))
        .arg("--state-dir")
        .arg(&h.dir)
        .args([
            "--wait",
            "--cpu",
            "shared",
            "--threads",
            "1",
            "--",
            "/bin/true",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !h.invoke(&["status", "2"]).status.success() {
        assert!(Instant::now() < until, "waiting client failed to submit");
        sleep(Duration::from_millis(50));
    }
    assert_eq!(h.job(2)["status"], "queued");
    h.run(&["drain"]);
    h.run(&["drain", "--wait"]);
    assert_eq!(h.job(1)["status"], "succeeded");
    assert_eq!(h.job(2)["status"], "queued");
    h.child.kill().unwrap();
    h.child.wait().unwrap();
    sleep(Duration::from_secs(1));
    h.start();
    sleep(Duration::from_millis(500));
    assert_eq!(h.job(2)["status"], "queued");
    let submitted = h.run(&["--cpu", "shared", "--threads", "1", "--", "/bin/true"]);
    assert_eq!(estimated_wait(&submitted), None);
    assert_eq!(h.job(3)["status"], "queued");
    h.run(&["resume"]);
    let result = waiter.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    h.wait_status(2, &["succeeded"]);
    h.wait_status(3, &["succeeded"]);
}

#[test]
fn gpu_cap_does_not_limit_cpu_jobs() {
    let h = Harness::new(&["--cpu-threads", "3", "--batch-ms", "0", "--max-shared", "1"]);
    for id in 1..=4 {
        h.run(&[
            "--cpu",
            "shared",
            "--threads",
            "1",
            "--",
            "/bin/sleep",
            "15",
        ]);
        if id <= 3 {
            assert_eq!(h.wait_status(id, &["running"])["threads"], 1);
        }
    }
    assert_eq!(h.job(4)["status"], "queued");
    h.run(&["--gpu", "shared", "--", "/bin/sleep", "15"]);
    h.wait_status(5, &["running"]);
    h.run(&["--gpu", "shared", "--", "/bin/sleep", "15"]);
    assert_eq!(h.job(6)["status"], "queued");
    for id in 1..=6 {
        h.run(&["cancel", &id.to_string()]);
        h.wait_status(id, &["cancelled"]);
    }
}

#[test]
fn estimate_counts_running_fixed_cpu_allocations() {
    let h = Harness::new(&["--cpu-threads", "4", "--batch-ms", "0"]);
    h.run(&[
        "--cpu",
        "shared",
        "--approx-execution-time",
        "8",
        "--",
        "/bin/sleep",
        "8",
    ]);
    h.wait_status(1, &["running"]);
    let output = h.run(&[
        "--cpu",
        "shared",
        "--approx-execution-time",
        "1",
        "--",
        "/bin/sleep",
        "4",
    ]);
    assert!((6..=8).contains(&estimated_wait(&output).unwrap()));
    assert_eq!(h.job(2)["status"], "queued");
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
    h.wait_status(2, &["running"]);
    h.run(&["cancel", "2"]);
    h.wait_status(2, &["cancelled"]);
}

#[test]
fn estimates_distinguish_independent_external_locks() {
    let h = Harness::new(&["--cpu-threads", "4", "--batch-ms", "0"]);
    let held = h.lock();
    let gpu = h.run(&["--gpu", "exclusive", "--", "/bin/sleep", "2"]);
    assert_eq!(estimated_wait(&gpu), None);
    let cpu = h.run(&["--cpu", "exclusive", "--", "/bin/sleep", "4"]);
    assert_eq!(estimated_wait(&cpu), Some(0));
    h.wait_status(2, &["running"]);
    assert_eq!(h.job(1)["status"], "queued");
    drop(held);
    h.wait_status(1, &["running"]);
    h.run(&["cancel", "1"]);
    h.run(&["cancel", "2"]);
    h.wait_status(1, &["cancelled"]);
    h.wait_status(2, &["cancelled"]);
}

#[test]
fn estimates_check_external_cpu_locks_while_gpu_jobs_run() {
    let h = Harness::new(&["--cpu-threads", "4", "--batch-ms", "0"]);
    h.run(&["--gpu", "shared", "--", "/bin/sleep", "5"]);
    h.wait_status(1, &["running"]);
    let cpu = OpenOptions::new()
        .read(true)
        .write(true)
        .open(h.dir.join("cpu.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(cpu.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let blocked = h.run(&["--cpu", "shared", "--", "/bin/sleep", "1"]);
    assert_eq!(estimated_wait(&blocked), None);
    let independent = h.run(&[
        "--gpu",
        "shared",
        "--approx-execution-time",
        "1",
        "--",
        "/bin/sleep",
        "1",
    ]);
    assert_eq!(estimated_wait(&independent), Some(0));
    h.wait_status(3, &["succeeded"]);
    drop(cpu);
    h.wait_status(2, &["succeeded"]);
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
}

#[test]
fn external_shared_cpu_lock_allows_shared_but_blocks_exclusive_estimates() {
    let h = Harness::new(&["--cpu-threads", "4", "--batch-ms", "0"]);
    let held = OpenOptions::new()
        .read(true)
        .write(true)
        .open(h.dir.join("cpu.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) },
        0
    );
    let exclusive = h.run(&[
        "--cpu",
        "exclusive",
        "--approx-execution-time",
        "10",
        "--",
        "/bin/sleep",
        "1",
    ]);
    assert_eq!(estimated_wait(&exclusive), None);
    let shared = h.run(&[
        "--cpu",
        "shared",
        "--approx-execution-time",
        "1",
        "--",
        "/bin/sleep",
        "1",
    ]);
    assert_eq!(estimated_wait(&shared), Some(0));
    h.wait_status(2, &["succeeded"]);
    assert_eq!(h.job(1)["status"], "queued");
    drop(held);
    h.wait_status(1, &["succeeded"]);
}

#[test]
fn cpu_batch_divides_threads_and_passes_environment() {
    let h = Harness::new(&["--cpu-threads", "61", "--batch-ms", "800"]);
    let gpu_lock = h.lock();
    for _ in 0..3 {
        h.run(&[
            "--cpu",
            "shared",
            "--",
            "/bin/sh",
            "-c",
            "echo $SOGOTF_THREADS; exec sleep 6",
        ]);
    }
    for id in 1..=3 {
        h.wait_status(id, &["running"]);
    }
    let counts: Vec<_> = (1..=3)
        .map(|id| h.job(id)["threads"].as_u64().unwrap())
        .collect();
    assert_eq!(counts, vec![21, 20, 20]);
    for id in 1..=3 {
        assert_eq!(h.job(id)["gpu"], false);
        assert_eq!(
            fs::read_to_string(h.dir.join(format!("{id}.stdout")))
                .unwrap()
                .trim(),
            counts[(id - 1) as usize].to_string()
        );
        h.run(&["cancel", &id.to_string()]);
    }
    for id in 1..=3 {
        h.wait_status(id, &["cancelled"]);
    }
    drop(gpu_lock);
}

#[test]
fn cpu_batch_never_allocates_more_threads_than_available() {
    let h = Harness::new(&["--cpu-threads", "2", "--batch-ms", "800"]);
    for _ in 0..3 {
        h.run(&["--cpu", "shared", "--", "/bin/sleep", "6"]);
    }
    assert_eq!(h.wait_status(1, &["running"])["threads"], 1);
    assert_eq!(h.wait_status(2, &["running"])["threads"], 1);
    assert_eq!(h.job(3)["status"], "queued");
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
    assert_eq!(h.wait_status(3, &["running"])["threads"], 1);
    h.run(&["cancel", "2"]);
    h.run(&["cancel", "3"]);
    h.wait_status(2, &["cancelled"]);
    h.wait_status(3, &["cancelled"]);
}

#[test]
fn fixed_and_adaptive_jobs_share_a_batch() {
    let h = Harness::new(&["--cpu-threads", "60", "--batch-ms", "1000"]);
    h.run(&[
        "--cpu",
        "shared",
        "--threads",
        "8",
        "--",
        "/bin/sh",
        "-c",
        "echo $SOGOTF_THREADS; exec sleep 6",
    ]);
    for _ in 0..2 {
        h.run(&["--cpu", "shared", "--", "/bin/sleep", "6"]);
    }
    let fixed = h.wait_status(1, &["running"]);
    assert_eq!(fixed["requested_threads"], 8);
    assert_eq!(fixed["threads"], 8);
    assert_eq!(h.wait_status(2, &["running"])["threads"], 26);
    assert_eq!(h.wait_status(3, &["running"])["threads"], 26);
    assert_eq!(
        fs::read_to_string(h.dir.join("1.stdout")).unwrap().trim(),
        "8"
    );
    for id in 1..=3 {
        h.run(&["cancel", &id.to_string()]);
    }
    for id in 1..=3 {
        h.wait_status(id, &["cancelled"]);
    }
}

#[test]
fn later_jobs_use_free_threads_without_changing_running_allocations() {
    let h = Harness::new(&["--cpu-threads", "60", "--batch-ms", "0"]);
    h.run(&[
        "--cpu",
        "shared",
        "--threads",
        "20",
        "--",
        "/bin/sleep",
        "8",
    ]);
    h.wait_status(1, &["running"]);
    h.run(&[
        "--cpu",
        "shared",
        "--threads",
        "30",
        "--",
        "/bin/sleep",
        "8",
    ]);
    h.wait_status(2, &["running"]);
    h.run(&["--cpu", "shared", "--", "/bin/sleep", "8"]);
    assert_eq!(h.wait_status(3, &["running"])["threads"], 10);
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
    assert_eq!(h.job(3)["threads"], 10);
    h.run(&["--cpu", "shared", "--", "/bin/sleep", "8"]);
    assert_eq!(h.wait_status(4, &["running"])["threads"], 20);
    for id in 2..=4 {
        h.run(&["cancel", &id.to_string()]);
    }
    for id in 2..=4 {
        h.wait_status(id, &["cancelled"]);
    }
}

#[test]
fn fixed_requests_wait_until_enough_threads_are_free() {
    let h = Harness::new(&["--cpu-threads", "60", "--batch-ms", "0"]);
    h.run(&[
        "--cpu",
        "shared",
        "--threads",
        "40",
        "--approx-execution-time",
        "8",
        "--",
        "/bin/sleep",
        "8",
    ]);
    assert_eq!(h.wait_status(1, &["running"])["threads"], 40);
    let output = h.run(&[
        "--cpu",
        "shared",
        "--threads",
        "30",
        "--approx-execution-time",
        "1",
        "--",
        "/bin/sleep",
        "4",
    ]);
    assert!((6..=8).contains(&estimated_wait(&output).unwrap()));
    assert_eq!(h.job(2)["status"], "queued");
    h.run(&["--gpu", "shared", "--", "/bin/sleep", "1"]);
    h.wait_status(3, &["succeeded"]);
    assert_eq!(h.job(2)["status"], "queued");
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
    assert_eq!(h.wait_status(2, &["running"])["threads"], 30);
    h.run(&["cancel", "2"]);
    h.wait_status(2, &["cancelled"]);
}

#[test]
fn fixed_exclusive_cpu_keeps_other_cpu_jobs_out() {
    let h = Harness::new(&["--cpu-threads", "60", "--batch-ms", "0"]);
    h.run(&[
        "--cpu",
        "exclusive",
        "--threads",
        "8",
        "--",
        "/bin/sleep",
        "6",
    ]);
    let job = h.wait_status(1, &["running"]);
    assert_eq!(job["threads"], 8);
    assert_eq!(job["requested_threads"], 8);
    h.run(&["--cpu", "shared", "--", "/bin/sleep", "6"]);
    assert_eq!(h.job(2)["status"], "queued");
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
    assert_eq!(h.wait_status(2, &["running"])["threads"], 60);
    h.run(&["cancel", "2"]);
    h.wait_status(2, &["cancelled"]);
}

#[test]
fn fixed_threads_work_with_legacy_exclusive_flag() {
    let h = Harness::new(&["--cpu-threads", "60"]);
    h.run(&["--exclusive", "--threads", "8", "--", "/bin/sleep", "6"]);
    let job = h.wait_status(1, &["running"]);
    assert_eq!(job["cpu"], "exclusive");
    assert_eq!(job["gpu"], true);
    assert_eq!(job["exclusive"], true);
    assert_eq!(job["threads"], 8);
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
}

#[test]
fn invalid_fixed_thread_requests_are_rejected() {
    let h = Harness::new(&["--cpu-threads", "60"]);
    for args in [
        vec!["--cpu", "shared", "--threads", "0", "--", "/bin/true"],
        vec!["--cpu", "shared", "--threads", "61", "--", "/bin/true"],
        vec!["--threads", "5", "--", "/bin/true"],
        vec!["--gpu", "shared", "--threads", "5", "--", "/bin/true"],
    ] {
        let output = h.invoke(&args);
        assert!(!output.status.success(), "request accepted: {args:?}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("thread"));
    }
    let mut stream = UnixStream::connect(h.dir.join("scheduler.sock")).unwrap();
    let request = serde_json::json!({"Submit": {"command": ["/bin/true"], "cwd": "/", "env": [], "estimate": 1, "gpu": true, "exclusive": false, "cpu": null, "requested_threads": 8, "resources_explicit": true}});
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).unwrap();
    let response: Value = serde_json::from_str(&response).unwrap();
    assert!(response["error"].as_str().unwrap().contains("CPU"));
    assert!(
        serde_json::from_slice::<Vec<Value>>(&h.run(&["list"]).stdout)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn fixed_request_survives_restart_and_is_revalidated() {
    let mut h = Harness::new(&["--cpu-threads", "60", "--batch-ms", "0"]);
    let held = OpenOptions::new()
        .read(true)
        .write(true)
        .open(h.dir.join("cpu.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    h.run(&["--cpu", "shared", "--threads", "8", "--", "/bin/sleep", "1"]);
    h.restart();
    let job = h.job(1);
    assert_eq!(job["status"], "queued");
    assert_eq!(job["requested_threads"], 8);
    h.extra = vec![
        "--cpu-threads".into(),
        "4".into(),
        "--batch-ms".into(),
        "0".into(),
    ];
    h.restart();
    let job = h.job(1);
    assert_eq!(job["status"], "failed");
    assert!(job["detail"].as_str().unwrap().contains("thread count"));
    drop(held);
}

#[test]
fn cpu_allocation_is_fixed_until_job_finishes() {
    let h = Harness::new(&["--cpu-threads", "60", "--batch-ms", "200"]);
    h.run(&["--cpu", "shared", "--", "/bin/sleep", "3"]);
    assert_eq!(h.wait_status(1, &["running"])["threads"], 60);
    h.run(&["--cpu", "shared", "--", "/bin/sleep", "1"]);
    assert_eq!(h.job(2)["status"], "queued");
    assert_eq!(h.job(1)["threads"], 60);
    h.wait_status(1, &["succeeded"]);
    assert_eq!(h.wait_status(2, &["running", "succeeded"])["threads"], 60);
    h.wait_status(2, &["succeeded"]);
}

#[test]
fn cpu_and_gpu_can_run_independently_and_exclusive_drains_both() {
    let h = Harness::new(&["--cpu-threads", "60", "--batch-ms", "0"]);
    h.run(&["--cpu", "exclusive", "--", "/bin/sleep", "4"]);
    let cpu = h.wait_status(1, &["running"]);
    assert_eq!(cpu["threads"], 60);
    assert_eq!(cpu["gpu"], false);
    h.run(&["--gpu", "shared", "--", "/bin/sleep", "4"]);
    h.wait_status(2, &["running"]);
    assert_eq!(h.job(1)["status"], "running");
    h.run(&[
        "--exclusive",
        "--approx-execution-time",
        "2",
        "--",
        "/bin/sleep",
        "1",
    ]);
    h.run(&[
        "--cpu",
        "shared",
        "--approx-execution-time",
        "1",
        "--",
        "/bin/sleep",
        "1",
    ]);
    assert_eq!(h.job(3)["status"], "queued");
    assert_eq!(h.job(4)["status"], "queued");
    let both = h.wait_status(3, &["running"]);
    assert_eq!(both["gpu"], true);
    assert_eq!(both["exclusive"], true);
    assert_eq!(both["cpu"], "exclusive");
    assert_eq!(both["threads"], 60);
    assert_eq!(h.job(4)["status"], "queued");
    h.wait_status(3, &["succeeded"]);
    h.wait_status(4, &["succeeded"]);
}

#[test]
fn exclusive_cpu_drain_allows_independent_gpu_jobs() {
    let h = Harness::new(&["--cpu-threads", "4", "--batch-ms", "0"]);
    h.run(&["--cpu", "shared", "--", "/bin/sleep", "5"]);
    h.wait_status(1, &["running"]);
    h.run(&[
        "--cpu",
        "exclusive",
        "--approx-execution-time",
        "2",
        "--",
        "/bin/sleep",
        "1",
    ]);
    h.run(&["--gpu", "shared", "--", "/bin/sleep", "1"]);
    h.wait_status(3, &["succeeded"]);
    assert_eq!(h.job(1)["status"], "running");
    assert_eq!(h.job(2)["status"], "queued");
    h.wait_status(2, &["succeeded"]);
}

#[test]
fn multi_resource_locks_are_acquired_together() {
    let h = Harness::new(&["--cpu-threads", "4", "--batch-ms", "0"]);
    let cpu = OpenOptions::new()
        .read(true)
        .write(true)
        .open(h.dir.join("cpu.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(cpu.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    h.run(&[
        "--gpu",
        "exclusive",
        "--cpu",
        "shared",
        "--",
        "/bin/sleep",
        "1",
    ]);
    assert_eq!(h.job(1)["status"], "queued");
    let gpu = h.lock();
    drop(cpu);
    assert_eq!(h.job(1)["status"], "queued");
    drop(gpu);
    let job = h.wait_status(1, &["running", "succeeded"]);
    assert_eq!(job["threads"], 4);
    h.wait_status(1, &["succeeded"]);
}

#[test]
fn cpu_and_gpu_locks_survive_scheduler_crash() {
    let mut h = Harness::new(&["--cpu-threads", "4", "--batch-ms", "0"]);
    h.run(&[
        "--gpu",
        "shared",
        "--cpu",
        "shared",
        "--",
        "/bin/sleep",
        "3",
    ]);
    h.wait_status(1, &["running"]);
    h.child.kill().unwrap();
    h.child.wait().unwrap();
    for name in ["device.lock", "cpu.lock"] {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(h.dir.join(name))
            .unwrap();
        assert_ne!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
    }
    let deadline = Instant::now() + Duration::from_secs(6);
    for name in ["device.lock", "cpu.lock"] {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(h.dir.join(name))
            .unwrap();
        while unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            assert!(Instant::now() < deadline, "{name} remained locked");
            sleep(Duration::from_millis(100));
        }
    }
}

#[test]
fn aliased_cpu_and_gpu_locks_are_rejected() {
    let h = Harness::new(&[]);
    let output = Command::new(env!("CARGO_BIN_EXE_sogotf"))
        .arg("--state-dir")
        .arg(h.dir.join("other"))
        .args(["serve", "--lock"])
        .arg(h.dir.join("device.lock"))
        .arg("--cpu-lock")
        .arg(h.dir.join("device.lock"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("locks must be distinct"));
}

#[test]
fn legacy_exclusive_takes_cpu_and_gpu() {
    let h = Harness::new(&["--cpu-threads", "60"]);
    let response = h.run(&["--exclusive", "--", "/bin/sleep", "3"]);
    assert!(String::from_utf8_lossy(&response.stdout).contains("estimated time until start: 0s"));
    let job = h.wait_status(1, &["running"]);
    assert_eq!(job["cpu"], "exclusive");
    assert_eq!(job["gpu"], true);
    assert_eq!(job["exclusive"], true);
    assert_eq!(job["threads"], 60);
    let cpu = OpenOptions::new()
        .read(true)
        .write(true)
        .open(h.dir.join("cpu.lock"))
        .unwrap();
    assert_ne!(
        unsafe { libc::flock(cpu.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) },
        0
    );
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
}

#[test]
fn legacy_exclusive_reports_unknown_while_external_cpu_lock_is_held() {
    let h = Harness::new(&[]);
    let cpu = OpenOptions::new()
        .read(true)
        .write(true)
        .open(h.dir.join("cpu.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(cpu.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let output = h.run(&["--exclusive", "--", "/bin/true"]);
    assert!(String::from_utf8_lossy(&output.stdout).contains("unknown"));
    assert_eq!(h.job(1)["status"], "queued");
    drop(cpu);
    h.wait_status(1, &["succeeded"]);
}

#[test]
fn old_queued_exclusive_job_migrates_to_both_resources() {
    let mut h = Harness::new(&[]);
    let gpu = h.lock();
    h.run(&["--exclusive", "--", "/bin/sleep", "1"]);
    let path = h.dir.join("state.json");
    let mut state: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let job = state["jobs"][0].as_object_mut().unwrap();
    for field in ["gpu", "cpu", "threads", "resources_explicit"] {
        job.remove(field);
    }
    fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();
    h.restart();
    let job = h.job(1);
    assert_eq!(job["gpu"], true);
    assert_eq!(job["cpu"], "exclusive");
    drop(gpu);
    h.wait_status(1, &["succeeded"]);
}

#[test]
fn old_client_exclusive_request_uses_both_resources() {
    let h = Harness::new(&["--cpu-threads", "6"]);
    let mut stream = UnixStream::connect(h.dir.join("scheduler.sock")).unwrap();
    let request = serde_json::json!({"Submit": {"command": ["/bin/sleep", "3"], "cwd": "/", "env": [], "estimate": 5, "exclusive": true}});
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).unwrap();
    let response: Value = serde_json::from_str(&response).unwrap();
    assert!(response["error"].is_null());
    let job = h.wait_status(1, &["running"]);
    assert_eq!(job["cpu"], "exclusive");
    assert_eq!(job["gpu"], true);
    assert_eq!(job["threads"], 6);
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
}

#[test]
fn recovery_waits_for_orphaned_cpu_locks() {
    let mut h = Harness::new(&["--cpu-threads", "4", "--batch-ms", "0"]);
    h.run(&["--cpu", "shared", "--", "/bin/sleep", "5"]);
    h.wait_status(1, &["running"]);
    h.restart();
    h.run(&["--cpu", "shared", "--", "/bin/true"]);
    assert_eq!(h.job(2)["status"], "queued");
    h.restart();
    assert_eq!(h.job(2)["status"], "queued");
    h.wait_status(2, &["succeeded"]);
}

#[test]
fn gpu_idle_eviction_leaves_other_cpu_jobs_running() {
    let h = Harness::new(&[
        "--cpu-threads",
        "4",
        "--batch-ms",
        "500",
        "--startup",
        "0",
        "--idle-threshold",
        "10",
        "--idle-seconds",
        "1",
        "--usage-command",
        "echo 0",
    ]);
    h.run(&[
        "--gpu",
        "exclusive",
        "--cpu",
        "shared",
        "--",
        "/bin/sleep",
        "8",
    ]);
    h.run(&["--cpu", "shared", "--", "/bin/sleep", "8"]);
    assert_eq!(h.wait_status(1, &["running"])["threads"], 2);
    assert_eq!(h.wait_status(2, &["running"])["threads"], 2);
    let job = h.wait_status(1, &["evicted"]);
    assert_eq!(job["detail"], "device utilization remained low");
    assert_eq!(h.job(2)["status"], "running");
    h.run(&["cancel", "2"]);
    h.wait_status(2, &["cancelled"]);
}

#[test]
fn cpu_only_exclusive_does_not_trigger_gpu_idle_eviction() {
    let h = Harness::new(&[
        "--cpu-threads",
        "4",
        "--startup",
        "0",
        "--idle-threshold",
        "10",
        "--idle-seconds",
        "1",
        "--usage-command",
        "echo 0",
    ]);
    h.run(&["--cpu", "exclusive", "--", "/bin/sleep", "4"]);
    h.wait_status(1, &["succeeded"]);
}

#[test]
fn cancellation_kills_uncooperative_process_group() {
    let h = Harness::new(&[]);
    h.run(&["--", "/bin/sh", "-c", "trap '' TERM; sleep 20 & wait"]);
    h.wait_status(1, &["running"]);
    h.run(&["cancel", "1"]);
    h.wait_status(1, &["cancelled"]);
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(h.dir.join("device.lock"))
        .unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        assert!(Instant::now() < until, "process group kept the lock");
        sleep(Duration::from_millis(50));
    }
}

#[test]
fn wait_returns_job_exit_code_and_captures_output() {
    let h = Harness::new(&[]);
    let output = Command::new(env!("CARGO_BIN_EXE_sogotf"))
        .arg("--state-dir")
        .arg(&h.dir)
        .args(["--wait", "--", "/bin/sh", "-c", "echo hello; exit 7"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(7));
    assert_eq!(
        fs::read_to_string(h.dir.join("1.stdout")).unwrap(),
        "hello\n"
    );
    assert_eq!(h.job(1)["status"], "failed");
}

#[test]
fn child_keeps_flock_after_scheduler_crash() {
    let mut h = Harness::new(&[]);
    h.run(&["--", "/bin/sleep", "3"]);
    h.wait_status(1, &["running"]);
    h.child.kill().unwrap();
    h.child.wait().unwrap();
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(h.dir.join("device.lock"))
        .unwrap();
    assert_ne!(
        unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let until = Instant::now() + Duration::from_secs(6);
    while unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        assert!(Instant::now() < until, "lock remained held after job exit");
        sleep(Duration::from_millis(100));
    }
}
