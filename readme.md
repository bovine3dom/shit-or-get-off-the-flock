# shit or get off the flock (sogotf)

dead simple queue for shared and exclusive CPU and GPU use. short jobs jump the queue

## build

```sh
cargo build --release
```

find the binary at `target/release/sogotf`. u might need to install `flock` or switch to linux if you have somehow got this far in life without already doing so

## run

start a scheduler with a GPU lock, a CPU lock and a thread budget:

```sh
sogotf serve --lock /tmp/gpu.lock --cpu-lock /tmp/cpu.lock --cpu-threads 60
```

the CPU lock defaults to `cpu.lock` in the state directory, and the thread budget defaults to the detected CPU count.

the scheduler runs in the foreground. probably best to run it as a service. other jobs can use the same lock:

```sh
flock /tmp/gpu.lock your-existing-command
```

jobs using the old interface share the GPU by default, with up to 10 running together. set another GPU job limit with `--max-shared`. the CPU thread budget limits concurrent CPU jobs. the GPU job limit counts jobs managed by this scheduler.

each scheduler manages one GPU lock and one CPU lock. use one scheduler for each CPU thread budget: separate schedulers share exclusive locks, but each keeps its own thread count. external CPU jobs can use the CPU lock in the same way as the GPU lock.

set the state directory with `--state-dir DIR` before the command, or with `SOGOTF_DIR`; the default is `~/.local/state/sogotf`. the directory must belong to the scheduler user and have mode `0700`.

## systemd

install the binary and make a directory for user services:

```sh
install -Dm755 target/release/sogotf "$HOME/.local/bin/sogotf"
mkdir -p "$HOME/.config/systemd/user"
```

save this as `~/.config/systemd/user/sogotf.service`:

```ini
[Unit]
Description=sogotf device queue

[Service]
ExecStart=%h/.local/bin/sogotf serve --lock /tmp/gpu.lock
Restart=on-failure

[Install]
WantedBy=default.target
```

```sh
systemctl --user daemon-reload
systemctl --user enable --now sogotf.service
systemctl --user status sogotf.service
```

add the eviction options from below to `ExecStart` if you want them. use `loginctl enable-linger "$USER"` to keep the user service running after logout.

systemd stops the service's running jobs during a direct stop or restart. use the drain procedure below to let the jobs finish first.

## jobs

```sh
# queue and get a job id and estimated wait. all times in seconds
sogotf --approx-execution-time 120 -- python train.py

# the old --exclusive flag takes both CPU and GPU exclusively
sogotf --exclusive --threads 1 --wait --approx-execution-time 10 -- ./render --frames 20

# CPU only, with an exclusive or shared allocation
sogotf --cpu exclusive -- sh -c 'exec make -j "$SOGOTF_THREADS"'
sogotf --cpu shared -- sh -c 'exec make -j "$SOGOTF_THREADS"'

# reserve threads for a program that uses a fixed number
sogotf --cpu shared --threads 8 -- ./fixed-thread-program

# choose each resource independently
sogotf --gpu shared --cpu shared --threads 1 -- ./train
sogotf --gpu exclusive --cpu shared --threads 1 -- ./train
sogotf --gpu exclusive --cpu exclusive --threads 1 -- ./train

sogotf list
sogotf status 1
sogotf cancel 1
```

`--cpu` and `--gpu` accept `shared` or `exclusive`. specifying only one requests only that resource; without either option, jobs use the GPU in shared mode. the old `--exclusive` flag requests both resources exclusively.

jobs get the submitter's working directory and environment, with stdout and stderr saved to `ID.stdout` and `ID.stderr` in the state directory. `--wait` returns the job's exit code when available. interrupting the client leaves the job queued. use `cancel` to stop a job.

`state.json` keeps the queue, job details and environment, so keep the directory private if your environment contains secrets. job records and output stay there until you remove them; stop the scheduler before removing state files.

## queue jumping

each job gets a target of submission time + 2 × estimated runtime, and the earliest target runs first. short jobs can jump ahead, while older jobs eventually take priority over new arrivals.

when an exclusive job reaches the front while shared jobs are running, the scheduler lets those jobs finish before starting it. new jobs needing the same resources wait, while jobs using independent resources can run.

the target is a goal, and running jobs, external lock holders or bad estimates can make you wait longer.

the estimated wait simulates running jobs, fixed thread allocations, batching and jobs ahead. an external lock can make the wait unknown, while independent jobs can still get an estimate. external shared holders can be hard to distinguish while sogotf holds a shared lock; new jobs and inaccurate runtimes can also change the wait.

give each job a runtime estimate in seconds, or let the scheduler use the mean of the last 10 completed jobs. when the history is empty, it uses the configured default; the estimate stays fixed after submission.

## CPU threads

shared CPU jobs ready together get an equal share of the free threads, with any remainder given to the first jobs in the queue. the scheduler waits `--batch-ms` (1000 by default) to collect a batch; `0` starts jobs immediately. exclusive CPU jobs reserve the whole CPU and get the full thread budget by default.

each CPU job receives its allocation as `SOGOTF_THREADS`, and `status` reports it in `threads`. the allocation stays fixed until the job exits, so a job that takes all the threads can make later jobs wait. the budget is cooperative: adaptive jobs use the variable to set their thread count, and fixed jobs use their reserved count.

CPU jobs must specify `--threads N` or include `SOGOTF_THREADS` in the submitted command. use `SOGOTF_THREADS` to set the thread or job count for programs that support variable parallelism. use `--threads N` for a program with a fixed thread count. `--threads` reserves exactly `N` threads; within each batch, the scheduler reserves fixed requests first and divides the rest among adaptive jobs. `status` shows the fixed request in `requested_threads`.

quote shell commands so `SOGOTF_THREADS` expands in the job. if a script reads the variable internally, reference it in the submitted command, for example `sh -c 'export SOGOTF_THREADS; exec ./script'`. the check examines the command arguments. the program must use its allocation correctly. queued jobs accepted before this check keep their place.

`--threads` requires `--cpu` or `--exclusive`, and `N` must be between 1 and the configured CPU thread budget. an exclusive CPU job still keeps the whole CPU to itself, even when its fixed count is smaller. if the CPU budget is reduced on restart, queued jobs that exceed the new budget fail.

## eviction

jobs get kicked off after `--max-runtime` or 3 × their estimated runtime, whichever comes first; the clock starts at launch. estimates above the maximum are rejected.

idle eviction applies to jobs with exclusive GPU access and is off by default. to kick those jobs off GPU 0 after utilization stays below 10% for 120 seconds:

```sh
sogotf serve --lock /tmp/gpu.lock \
  --startup 60 --idle-threshold 10 --idle-seconds 120 \
  --usage-command 'nvidia-smi -i 0 --query-gpu=utilization.gpu --format=csv,noheader,nounits'
```

the usage command prints one percentage from 0 to 100, sampled about once per second after the startup grace period. a threshold of `0` turns idle eviction off, and a failed, timed-out or invalid sample resets the idle timer.

device utilization includes other processes, and jobs can go quiet while loading data or waiting for I/O, so pick a threshold and duration that fit your workload.

| option | default | what it does |
| --- | --- | --- |
| `--lock PATH` | required | GPU `flock` file |
| `--cpu-lock PATH` | state directory `cpu.lock` | CPU `flock` file |
| `--cpu-threads THREADS` | detected CPU count | CPU thread budget |
| `--batch-ms MILLISECONDS` | 1000 | shared CPU batching window |
| `--max-shared JOBS` | 10 | maximum concurrent shared GPU jobs |
| `--max-runtime SECONDS` | 86400 | maximum job runtime |
| `--default-estimate SECONDS` | 3600 | estimate before history exists |
| `--startup SECONDS` | 60 | grace period before idle sampling |
| `--idle-threshold PERCENT` | 0 | idle eviction threshold |
| `--idle-seconds SECONDS` | 60 | time below threshold before eviction |
| `--usage-command COMMAND` | unset | shell command that prints utilization |

## safe restart

pause the queue and wait for running jobs and recovery to finish:

```sh
sogotf drain --wait
systemctl --user restart sogotf.service
sogotf resume
```

`drain` also works without `--wait`. it pauses new starts and reports when the scheduler is drained. submitted jobs stay queued. the pause stays active across restarts. use `resume` to start queued jobs again. waiting clients reconnect while the scheduler restarts.

for an upgrade, install the new binary with an atomic rename before the restart:

```sh
install -m755 target/release/sogotf "$HOME/.local/bin/sogotf.new"
mv "$HOME/.local/bin/sogotf.new" "$HOME/.local/bin/sogotf"
```

the running scheduler must support `drain` for this procedure. for a foreground scheduler, stop and start it after the drain completes, then resume the queue.

## cleanup and crashes

on cancellation or eviction, the scheduler sends `SIGTERM` to the job's process group, waits up to two seconds, then sends `SIGKILL`. it also stops remaining processes in the group when the main process exits, so keep device-using processes in that group for supervision.

jobs inherit the lock descriptor, so a job can keep running and hold the lock if the scheduler dies. the lock stays held until its processes close the descriptor or exit.

on restart, the scheduler marks starting or running jobs as interrupted and waits for both locks to become free before continuing the queue. interrupted jobs need manual supervision, and processes that close the descriptors or escape the group need their own cleanup. exclusive jobs queued with an older version take both CPU and GPU exclusively.

## tests

```sh
cargo test
```
