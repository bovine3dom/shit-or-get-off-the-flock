# shit or get off the flock (sogotf)

dead simple queue for shared and exclusive device use. short jobs jump the queue

## build

```sh
cargo build --release
```

find the binary at `target/release/sogotf`. u might need to install `flock` or switch to linux if you have somehow got this far in life without already doing so

## run

one scheduler per lock:

```sh
sogotf serve --lock /tmp/gpu.lock
```

the scheduler runs in the foreground. probably best to run it as a service. other jobs can use the same lock:

```sh
flock /tmp/gpu.lock your-existing-command
```

jobs share the device by default, with up to 10 running together. set another limit with `--max-shared`; external shared lock holders use additional slots. existing exclusive lock users wait for shared jobs to finish.

for multiple devices, give each scheduler its own lock and state directory. set the directory with `--state-dir DIR` before the command, or with `SOGOTF_DIR`; the default is `~/.local/state/sogotf`. the directory must belong to the scheduler user and have mode `0700`.

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

systemd stops the service's running jobs when you stop or restart the service; the scheduler marks them as interrupted on its next start.

## jobs

```sh
# queue and get a job id and estimated wait. all times in seconds
sogotf --approx-execution-time 120 -- python train.py

# queue an exclusive job and wait for the result
sogotf --exclusive --wait --approx-execution-time 10 -- ./render --frames 20

sogotf list
sogotf status 1
sogotf cancel 1
```

jobs get the submitter's working directory and environment, with stdout and stderr saved to `ID.stdout` and `ID.stderr` in the state directory. `--wait` returns the job's exit code when available. interrupting the client leaves the job queued. use `cancel` to stop a job.

`state.json` keeps the queue, job details and environment, so keep the directory private if your environment contains secrets. job records and output stay there until you remove them; stop the scheduler before removing state files.

## queue jumping

each job gets a target of submission time + 2 × estimated runtime, and the earliest target runs first. short jobs can jump ahead, while older jobs eventually take priority over new arrivals.

when an exclusive job reaches the front while shared jobs are running, the scheduler lets those jobs finish before starting it. new shared jobs wait until the exclusive job has run.

the target is a goal, and running jobs, external lock holders or bad estimates can make you wait longer.

the estimated wait accounts for running jobs, shared slots and jobs ahead; new jobs can change it, and an external lock holder can make it unknown.

give each job a runtime estimate in seconds, or let the scheduler use the mean of the last 10 completed jobs. when the history is empty, it uses the configured default; the estimate stays fixed after submission.

## eviction

jobs get kicked off after `--max-runtime` or 3 × their estimated runtime, whichever comes first; the clock starts at launch. estimates above the maximum are rejected.

idle eviction applies to exclusive jobs and is off by default. to kick exclusive jobs off GPU 0 after utilization stays below 10% for 120 seconds:

```sh
sogotf serve --lock /tmp/gpu.lock \
  --startup 60 --idle-threshold 10 --idle-seconds 120 \
  --usage-command 'nvidia-smi -i 0 --query-gpu=utilization.gpu --format=csv,noheader,nounits'
```

the usage command prints one percentage from 0 to 100, sampled about once per second after the startup grace period. a threshold of `0` turns idle eviction off, and a failed, timed-out or invalid sample resets the idle timer.

device utilization includes other processes, and jobs can go quiet while loading data or waiting for I/O, so pick a threshold and duration that fit your workload.

| option | default | what it does |
| --- | --- | --- |
| `--lock PATH` | required | shared `flock` file |
| `--max-shared JOBS` | 10 | maximum concurrent shared jobs |
| `--max-runtime SECONDS` | 86400 | maximum job runtime |
| `--default-estimate SECONDS` | 3600 | estimate before history exists |
| `--startup SECONDS` | 60 | grace period before idle sampling |
| `--idle-threshold PERCENT` | 0 | idle eviction threshold |
| `--idle-seconds SECONDS` | 60 | time below threshold before eviction |
| `--usage-command COMMAND` | unset | shell command that prints utilization |

## cleanup and crashes

on cancellation or eviction, the scheduler sends `SIGTERM` to the job's process group, waits up to two seconds, then sends `SIGKILL`. it also stops remaining processes in the group when the main process exits, so keep device-using processes in that group for supervision.

jobs inherit the lock descriptor, so a job can keep running and hold the lock if the scheduler dies. the lock stays held until its processes close the descriptor or exit.

on restart, the scheduler marks starting or running jobs as interrupted and waits for the device lock to become free before continuing the queue. interrupted jobs need manual supervision, and processes that close the descriptor or escape the group need their own cleanup. jobs queued with an older version keep exclusive access.

## tests

```sh
cargo test
```
