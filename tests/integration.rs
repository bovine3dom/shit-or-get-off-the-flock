use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Harness {
    dir: PathBuf,
    child: Child,
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
        let harness = Self { dir, child };
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
        self.child = Command::new(env!("CARGO_BIN_EXE_sogotf"))
            .arg("--state-dir")
            .arg(&self.dir)
            .args(["serve", "--lock"])
            .arg(self.dir.join("device.lock"))
            .args(["--max-runtime", "20", "--default-estimate", "5"])
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
    fn run(&self, args: &[&str]) -> Output {
        let output = Command::new(env!("CARGO_BIN_EXE_sogotf"))
            .arg("--state-dir")
            .arg(&self.dir)
            .args(args)
            .output()
            .unwrap();
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
    assert!(String::from_utf8_lossy(&first.stdout)
        .contains("unknown (external lock held or recovery pending)"));
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
    h.restart();
    assert_eq!(h.job(2)["status"], "queued");
    h.wait_status(2, &["succeeded"]);
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
