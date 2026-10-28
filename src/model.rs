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
    #[serde(default = "legacy_exclusive")]
    pub exclusive: bool,
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
    #[serde(default)]
    pub recovery_pending: bool,
}

fn legacy_exclusive() -> bool {
    true
}

#[derive(Serialize, Deserialize)]
pub enum Request {
    Submit {
        command: Vec<String>,
        cwd: String,
        env: Vec<(String, String)>,
        estimate: Option<u64>,
        #[serde(default = "legacy_exclusive")]
        exclusive: bool,
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

pub fn estimate_wait(
    state: &State,
    id: u64,
    active: &[(u64, bool)],
    max_shared: usize,
    reserved: Option<u64>,
) -> Option<u64> {
    let mut active = active.to_vec();
    let mut jobs = ordered(state);
    if let Some(index) = jobs.iter().position(|j| Some(j.id) == reserved) {
        let job = jobs.remove(index);
        jobs.insert(0, job);
    }
    let mut time = 0;
    for job in jobs {
        active.retain(|(end, _)| *end > time);
        if job.exclusive || active.iter().any(|(_, exclusive)| *exclusive) {
            time = active.iter().map(|(end, _)| *end).max().unwrap_or(time);
            active.clear();
        } else if active.len() >= max_shared {
            time = active.iter().map(|(end, _)| *end).min().unwrap();
            active.retain(|(end, _)| *end > time);
        }
        if job.id == id {
            return Some(time);
        }
        active.push((time.saturating_add(job.estimate), job.exclusive));
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
            exclusive: true,
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
            recovery_pending: false,
            jobs: vec![job(1, 0, 100), job(2, 10, 5), job(3, 201, 1)],
        };
        assert_eq!(
            ordered(&state).iter().map(|j| j.id).collect::<Vec<_>>(),
            vec![2, 1, 3]
        );
        assert_eq!(estimate_wait(&state, 1, &[(7, true)], 10, None), Some(12));
        assert_eq!(estimate_wait(&state, 3, &[(7, true)], 10, None), Some(112));
    }

    #[test]
    fn old_jobs_remain_exclusive() {
        let mut value = serde_json::to_value(job(1, 0, 5)).unwrap();
        value.as_object_mut().unwrap().remove("exclusive");
        assert!(serde_json::from_value::<Job>(value).unwrap().exclusive);
        let request = serde_json::json!({"Submit": {"command": ["true"], "cwd": "/", "env": [], "estimate": null}});
        assert!(matches!(
            serde_json::from_value::<Request>(request).unwrap(),
            Request::Submit {
                exclusive: true,
                ..
            }
        ));
    }

    #[test]
    fn estimates_parallel_slots_and_exclusive_draining() {
        let mut state = State {
            jobs: vec![job(1, 0, 3), job(2, 1, 4), job(3, 2, 5)],
            ..State::default()
        };
        state.jobs[0].exclusive = false;
        state.jobs[1].exclusive = false;
        assert_eq!(estimate_wait(&state, 2, &[(5, false)], 2, None), Some(3));
        assert_eq!(estimate_wait(&state, 3, &[(5, false)], 2, None), Some(7));
        assert_eq!(
            estimate_wait(&state, 2, &[(5, false)], 2, Some(3)),
            Some(10)
        );
    }
}
