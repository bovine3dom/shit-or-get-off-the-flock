dead simple script to queue up and share exclusive device usage

config

1) path to underlying flock
2) command to get device usage
3) eviction threshold, when device usage falls below x% for y seconds
4) startup time
5) max job length

interface

```
sogotf [--wait] [--approx-execution-time SECONDS] /path/to/command
```

returns estimate in seconds of current wait time, calculated by sum(ifnull(approx execution time,mean(actual execution time of past 10 jobs)))

throws error if approx execution time is longer than max job length. jobs are evicted if they take more than 3x their estimated time.

queuing aims to complete jobs within 2x their approx execution time, so short jobs can jump the queue
