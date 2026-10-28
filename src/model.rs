use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Queued,
    Starting,
    Running,
    Stopping,
    Succeeded,
    Failed,
    Cancelled,
    Evicted,
    Interrupted,
}

impl Status {
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Evicted | Self::Interrupted
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Job {
    pub id: u64,
    pub command: Vec<String>,
    pub cwd: String,
    pub env: Vec<(String, String)>,
    pub submitted: u64,
    pub estimate: u64,
    pub started: Option<u64>,
    pub finished: Option<u64>,
    pub status: Status,
    pub exit_code: Option<i32>,
    pub detail: Option<String>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct State {
    pub next_id: u64,
    pub jobs: Vec<Job>,
    pub durations: Vec<u64>,
}

#[derive(Serialize, Deserialize)]
pub enum Request {
    Submit {
        command: Vec<String>,
        cwd: String,
        env: Vec<(String, String)>,
        estimate: Option<u64>,
    },
    List,
    Get {
        id: u64,
    },
    Cancel {
        id: u64,
    },
}

#[derive(Serialize, Deserialize)]
pub struct Response {
    pub error: Option<String>,
    pub jobs: Vec<Job>,
    pub wait_seconds: Option<u64>,
}

impl Response {
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            error: Some(message.into()),
            jobs: vec![],
            wait_seconds: None,
        }
    }
    pub fn jobs(mut jobs: Vec<Job>) -> Self {
        for job in &mut jobs {
            job.env.clear();
        }
        Self {
            error: None,
            jobs,
            wait_seconds: None,
        }
    }
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn priority(job: &Job) -> (u64, u64, u64) {
    (
        job.submitted.saturating_add(job.estimate.saturating_mul(2)),
        job.submitted,
        job.id,
    )
}

pub fn ordered(state: &State) -> Vec<&Job> {
    let mut jobs: Vec<_> = state
        .jobs
        .iter()
        .filter(|j| j.status == Status::Queued)
        .collect();
    jobs.sort_by_key(|j| priority(j));
    jobs
}

pub fn estimate_wait(state: &State, id: u64, current_remaining: u64) -> Option<u64> {
    let mut wait = current_remaining;
    for job in ordered(state) {
        if job.id == id {
            return Some(wait);
        }
        wait = wait.saturating_add(job.estimate);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    fn job(id: u64, submitted: u64, estimate: u64) -> Job {
        Job {
            id,
            command: vec![],
            cwd: String::new(),
            env: vec![],
            submitted,
            estimate,
            started: None,
            finished: None,
            status: Status::Queued,
            exit_code: None,
            detail: None,
        }
    }
    #[test]
    fn short_jobs_jump_and_old_jobs_gain_priority() {
        let state = State {
            next_id: 3,
            durations: vec![],
            jobs: vec![job(1, 0, 100), job(2, 10, 5), job(3, 201, 1)],
        };
        assert_eq!(
            ordered(&state).iter().map(|j| j.id).collect::<Vec<_>>(),
            vec![2, 1, 3]
        );
        assert_eq!(estimate_wait(&state, 1, 7), Some(12));
        assert_eq!(estimate_wait(&state, 3, 7), Some(112));
    }
}
