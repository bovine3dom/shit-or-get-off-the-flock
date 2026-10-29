use crate::model::{Access, Job};
use std::io;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resources {
    pub gpu: Option<Access>,
    pub cpu: Option<Access>,
    pub threads: usize,
}

impl Resources {
    pub fn job(job: &Job) -> Self {
        Self {
            gpu: job.gpu.then_some(if job.exclusive {
                Access::Exclusive
            } else {
                Access::Shared
            }),
            cpu: job.cpu,
            threads: job.requested_threads.unwrap_or(0),
        }
    }
    fn overlaps(self, other: Self) -> bool {
        (self.gpu.is_some() && other.gpu.is_some()) || (self.cpu.is_some() && other.cpu.is_some())
    }
    fn exclusive(self) -> bool {
        self.gpu == Some(Access::Exclusive) || self.cpu == Some(Access::Exclusive)
    }
}

#[derive(Clone, Copy)]
pub struct Limits {
    pub max_shared: usize,
    pub cpu_threads: usize,
    pub batch_ms: u64,
}

#[derive(Clone, Copy)]
pub struct Queued {
    pub id: u64,
    pub resources: Resources,
    pub duration_ms: u64,
}

impl Queued {
    pub fn job(job: &Job) -> Self {
        Self {
            id: job.id,
            resources: Resources::job(job),
            duration_ms: job.estimate.saturating_mul(1000),
        }
    }
}

pub struct Planned<L> {
    pub id: u64,
    pub resources: Resources,
    pub locks: L,
    fixed_threads: bool,
}

fn fits(request: Resources, active: impl Iterator<Item = Resources>, limits: Limits) -> bool {
    let active: Vec<_> = active.collect();
    if let Some(mode) = request.gpu {
        let gpu: Vec<_> = active.iter().filter_map(|r| r.gpu).collect();
        if (mode == Access::Exclusive && !gpu.is_empty())
            || gpu.contains(&Access::Exclusive)
            || gpu.len() >= limits.max_shared
        {
            return false;
        }
    }
    if let Some(mode) = request.cpu {
        let cpu: Vec<_> = active.iter().filter(|r| r.cpu.is_some()).collect();
        if (mode == Access::Exclusive && !cpu.is_empty())
            || cpu.iter().any(|r| r.cpu == Some(Access::Exclusive))
            || cpu.len() >= limits.max_shared
            || request.threads
                > limits
                    .cpu_threads
                    .saturating_sub(cpu.iter().map(|r| r.threads).sum::<usize>())
        {
            return false;
        }
    }
    true
}

// The live scheduler and estimator use the same admission, draining and batching rules.
pub fn plan<L>(
    queue: &[Queued],
    active: &[Resources],
    draining: &mut Option<u64>,
    batch_deadline: &mut Option<u64>,
    now: u64,
    limits: Limits,
    mut locks: impl FnMut(Resources) -> io::Result<Option<L>>,
) -> io::Result<Vec<Planned<L>>> {
    let mut jobs = queue.to_vec();
    if let Some(index) = jobs.iter().position(|j| Some(j.id) == *draining) {
        let job = jobs.remove(index);
        jobs.insert(0, job);
    } else {
        *draining = None;
    }
    let reserved = draining
        .and_then(|id| jobs.iter().find(|j| j.id == id))
        .map(|j| j.resources);
    let mut planned: Vec<Planned<L>> = Vec::new();
    let mut blocked_gpu = false;
    let mut blocked_cpu = false;
    for job in &jobs {
        let mut request = job.resources;
        let fixed_threads = request.threads > 0;
        if let Some(cpu) = request.cpu {
            if !fixed_threads {
                request.threads = if cpu == Access::Exclusive {
                    limits.cpu_threads
                } else {
                    1
                };
            }
        }
        if Some(job.id) != *draining && reserved.is_some_and(|r| r.overlaps(request)) {
            continue;
        }
        if (blocked_gpu && request.gpu.is_some()) || (blocked_cpu && request.cpu.is_some()) {
            continue;
        }
        if !fits(
            request,
            active
                .iter()
                .copied()
                .chain(planned.iter().map(|p| p.resources)),
            limits,
        ) {
            if draining.is_none()
                && request.exclusive()
                && active.iter().any(|r| r.overlaps(request))
            {
                *draining = Some(job.id);
            }
            blocked_gpu |= request.gpu.is_some();
            blocked_cpu |= request.cpu.is_some();
            continue;
        }
        let Some(locks) = locks(request)? else {
            blocked_gpu |= request.gpu.is_some();
            blocked_cpu |= request.cpu.is_some();
            continue;
        };
        if request.cpu == Some(Access::Shared) {
            let deadline =
                batch_deadline.get_or_insert_with(|| now.saturating_add(limits.batch_ms));
            if now < *deadline {
                blocked_gpu |= request.gpu.is_some();
                blocked_cpu = true;
                continue;
            }
        }
        planned.push(Planned {
            id: job.id,
            resources: request,
            locks,
            fixed_threads,
        });
    }
    let shared = planned
        .iter()
        .filter(|p| p.resources.cpu == Some(Access::Shared))
        .count();
    if shared > 0 {
        let adaptive = planned
            .iter()
            .filter(|p| p.resources.cpu == Some(Access::Shared) && !p.fixed_threads)
            .count();
        if adaptive > 0 {
            let used: usize = active.iter().map(|r| r.threads).sum::<usize>()
                + planned
                    .iter()
                    .filter(|p| p.resources.cpu == Some(Access::Shared) && p.fixed_threads)
                    .map(|p| p.resources.threads)
                    .sum::<usize>();
            let free = limits.cpu_threads - used;
            let mut remainder = free % adaptive;
            for plan in &mut planned {
                if plan.resources.cpu == Some(Access::Shared) && !plan.fixed_threads {
                    plan.resources.threads = free / adaptive + usize::from(remainder > 0);
                    remainder = remainder.saturating_sub(1);
                }
            }
        }
        *batch_deadline = None;
    } else if !jobs.iter().any(|j| j.resources.cpu == Some(Access::Shared)) {
        *batch_deadline = None;
    }
    if planned.iter().any(|p| Some(p.id) == *draining) {
        *draining = None;
    }
    Ok(planned)
}

#[derive(Clone, Copy)]
pub struct Timed {
    pub resources: Resources,
    pub end_ms: u64,
}

pub fn estimate_wait(
    mut queue: Vec<Queued>,
    target: u64,
    mut active: Vec<Timed>,
    limits: Limits,
    mut draining: Option<u64>,
    mut batch_deadline: Option<u64>,
    external: Resources,
) -> Option<u64> {
    let mut now = 0u64;
    loop {
        active.retain(|a| a.end_ms > now);
        let resources: Vec<_> = active.iter().map(|a| a.resources).collect();
        let planned = plan(
            &queue,
            &resources,
            &mut draining,
            &mut batch_deadline,
            now,
            limits,
            |request| {
                let conflicts = |wanted: Option<Access>, held: Option<Access>| {
                    wanted.is_some()
                        && held.is_some()
                        && (wanted == Some(Access::Exclusive) || held == Some(Access::Exclusive))
                };
                Ok(
                    (!conflicts(request.gpu, external.gpu)
                        && !conflicts(request.cpu, external.cpu))
                    .then_some(()),
                )
            },
        )
        .ok()?;
        if planned.iter().any(|job| job.id == target) {
            return Some(now.div_ceil(1000));
        }
        if !planned.is_empty() {
            for job in planned {
                let index = queue.iter().position(|j| j.id == job.id)?;
                let queued = queue.remove(index);
                active.push(Timed {
                    resources: job.resources,
                    end_ms: now.checked_add(queued.duration_ms)?,
                });
            }
            continue;
        }
        now = active
            .iter()
            .map(|a| a.end_ms)
            .filter(|&t| t > now)
            .chain(batch_deadline.filter(|&t| t > now))
            .min()?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Access::{Exclusive as X, Shared as S};

    fn r(gpu: Option<Access>, cpu: Option<Access>, threads: usize) -> Resources {
        Resources { gpu, cpu, threads }
    }
    fn q(id: u64, gpu: Option<Access>, cpu: Option<Access>, seconds: u64) -> Queued {
        Queued {
            id,
            resources: r(gpu, cpu, 0),
            duration_ms: seconds * 1000,
        }
    }
    fn fixed(mut job: Queued, threads: usize) -> Queued {
        job.resources.threads = threads;
        job
    }
    fn limits() -> Limits {
        Limits {
            max_shared: 10,
            cpu_threads: 60,
            batch_ms: 1000,
        }
    }
    fn no_external() -> Resources {
        r(None, None, 0)
    }
    fn estimate(queue: Vec<Queued>, target: u64, active: Vec<Timed>) -> Option<u64> {
        estimate_wait(queue, target, active, limits(), None, None, no_external())
    }

    #[test]
    fn shared_cpu_batch_splits_threads_and_starts_together() {
        let queue = vec![
            q(1, None, Some(S), 3),
            q(2, None, Some(S), 10),
            q(3, None, Some(S), 4),
        ];
        assert_eq!(estimate(queue.clone(), 3, vec![]), Some(1));
        let planned = plan(
            &queue,
            &[],
            &mut None,
            &mut Some(1000),
            1000,
            limits(),
            |_| Ok(Some(())),
        )
        .unwrap();
        assert_eq!(
            planned
                .iter()
                .map(|j| j.resources.threads)
                .collect::<Vec<_>>(),
            vec![20, 20, 20]
        );
    }

    #[test]
    fn fixed_thread_jobs_reserve_before_adaptive_split() {
        let queue = vec![
            q(1, None, Some(S), 1),
            fixed(q(2, None, Some(S), 1), 8),
            q(3, None, Some(S), 1),
        ];
        let planned = plan(
            &queue,
            &[],
            &mut None,
            &mut Some(1000),
            1000,
            limits(),
            |_| Ok(Some(())),
        )
        .unwrap();
        assert_eq!(
            planned
                .iter()
                .map(|j| j.resources.threads)
                .collect::<Vec<_>>(),
            vec![26, 8, 26]
        );
        let queue = vec![
            fixed(q(1, None, Some(S), 1), 20),
            fixed(q(2, None, Some(S), 1), 10),
        ];
        let planned = plan(
            &queue,
            &[],
            &mut None,
            &mut Some(1000),
            1000,
            limits(),
            |_| Ok(Some(())),
        )
        .unwrap();
        assert_eq!(
            planned
                .iter()
                .map(|j| j.resources.threads)
                .collect::<Vec<_>>(),
            vec![20, 10]
        );
    }

    #[test]
    fn fixed_requests_wait_for_sufficient_threads_and_exclusive_stays_exclusive() {
        let queue = vec![
            fixed(q(1, None, Some(S), 3), 40),
            fixed(q(2, None, Some(S), 1), 30),
        ];
        assert_eq!(estimate(queue, 2, vec![]), Some(5));
        let queue = vec![fixed(q(1, None, Some(X), 3), 8), q(2, None, Some(S), 1)];
        assert_eq!(estimate(queue.clone(), 2, vec![]), Some(4));
        let planned = plan(&queue, &[], &mut None, &mut None, 0, limits(), |_| {
            Ok(Some(()))
        })
        .unwrap();
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].resources.threads, 8);
    }

    #[test]
    fn fixed_cpu_allocations_and_batch_delay_affect_wait() {
        let active = vec![Timed {
            resources: r(None, Some(S), 60),
            end_ms: 4000,
        }];
        assert_eq!(estimate(vec![q(1, None, Some(S), 3)], 1, active), Some(5));
        let queue = vec![
            q(1, None, Some(S), 3),
            q(2, None, Some(S), 10),
            q(3, None, Some(X), 2),
        ];
        assert_eq!(estimate(queue, 3, vec![]), Some(11));
    }

    #[test]
    fn independent_resources_run_together() {
        let active = vec![Timed {
            resources: r(Some(X), None, 0),
            end_ms: 10000,
        }];
        assert_eq!(estimate(vec![q(1, None, Some(X), 3)], 1, active), Some(0));
        let active = vec![Timed {
            resources: r(None, Some(X), 60),
            end_ms: 10000,
        }];
        assert_eq!(estimate(vec![q(1, Some(S), None, 3)], 1, active), Some(0));
    }

    #[test]
    fn mixed_request_waits_for_gpu_then_batches_cpu() {
        let active = vec![
            Timed {
                resources: r(None, Some(S), 30),
                end_ms: 4000,
            },
            Timed {
                resources: r(Some(S), None, 0),
                end_ms: 7000,
            },
        ];
        assert_eq!(
            estimate(vec![q(1, Some(X), Some(S), 1)], 1, active),
            Some(8)
        );
    }

    #[test]
    fn reservation_drains_both_resources_but_allows_independent_work() {
        let active = vec![
            Timed {
                resources: r(None, Some(S), 30),
                end_ms: 10000,
            },
            Timed {
                resources: r(Some(S), None, 0),
                end_ms: 4000,
            },
        ];
        let queue = vec![q(1, Some(X), Some(X), 3), q(2, None, Some(S), 1)];
        assert_eq!(
            estimate_wait(queue, 2, active, limits(), Some(1), None, no_external()),
            Some(14)
        );
        let active = vec![Timed {
            resources: r(Some(S), None, 0),
            end_ms: 10000,
        }];
        let queue = vec![q(1, Some(X), None, 3), q(2, None, Some(X), 1)];
        assert_eq!(
            estimate_wait(queue, 2, active, limits(), Some(1), None, no_external()),
            Some(0)
        );
    }

    #[test]
    fn external_locks_block_only_jobs_that_depend_on_them() {
        let external = r(Some(X), None, 0);
        assert_eq!(
            estimate_wait(
                vec![q(1, None, Some(S), 1)],
                1,
                vec![],
                limits(),
                None,
                None,
                external
            ),
            Some(1)
        );
        assert_eq!(
            estimate_wait(
                vec![q(1, Some(S), None, 1)],
                1,
                vec![],
                limits(),
                None,
                None,
                external
            ),
            None
        );
        let external = r(None, Some(S), 0);
        assert_eq!(
            estimate_wait(
                vec![q(1, None, Some(S), 1)],
                1,
                vec![],
                limits(),
                None,
                None,
                external
            ),
            Some(1)
        );
        assert_eq!(
            estimate_wait(
                vec![q(1, None, Some(X), 1)],
                1,
                vec![],
                limits(),
                None,
                None,
                external
            ),
            None
        );
        let queue = vec![q(1, Some(X), Some(X), 1), q(2, None, Some(S), 1)];
        assert_eq!(
            estimate_wait(queue, 2, vec![], limits(), None, None, r(Some(X), None, 0)),
            None
        );
    }

    #[test]
    fn existing_batch_deadline_and_shared_slot_limit_are_included() {
        let queue = vec![q(1, None, Some(S), 1)];
        assert_eq!(
            estimate_wait(queue, 1, vec![], limits(), None, Some(250), no_external()),
            Some(1)
        );
        let mut cap = limits();
        cap.max_shared = 1;
        let queue = vec![q(1, None, Some(S), 3), q(2, None, Some(S), 1)];
        assert_eq!(
            estimate_wait(queue, 2, vec![], cap, None, None, no_external()),
            Some(5)
        );
    }
}
