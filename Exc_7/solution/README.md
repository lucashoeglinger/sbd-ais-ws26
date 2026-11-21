# 1. Decision: Architecture B ("The lake is the truth")

I chose **Architecture B**, implemented as:

```python
def run_once():
    start = lake_position()
    new = read_new(start)
    run = write_files(new, STAGING)
    publish(run)
```

## Why Architecture B over A or C?

In Architecture A (state files), we have a dual write problem (It requires 2 seperate steps). If a run stops halfway it can cause duplicates on retry or corrupted reads. IF the run crashes after writing to the file but before `save_state()` runs, the statefile still holds the old offset, causing the next run to reread the old files and the entire batch

Architecture C(Duplicate on read) allows for duplicates to accumulate physically in the lake whenever a run retires or crashes. It shifts the strain on the consumers, it forces all its consumers to scan through `DEDUP_SQL`, which is implemented as `SELECT DISTINCT ON (kafka_partition, kafka_offset)`, instead of reading the raw `.parquet` files directly

Architecture B eliminates the dual-write problem by making the lake itself the single source of truth (`lake_position()`) and using a staging directory (`STAGING`) with an atomic rename (`publish()`). Data and offset state become visible to readers in a single atomic filesystem operation (`os.rename`), guaranteeing true physical exactly-once delivery without requiring special read views.


# 2. Explanation of Results
First run: `lake_position()` sees no files in `LAKE` and returns None. `read_new(None)` reads from offset 0 up to the current end of all 4 partitions, writes them to `STAGING`, and atomically moves the run folder into `LAKE`.

New records arrive: `lake_position()` queries `MAX(kafka_offset) + 1` per partition from LAKE and reads strictly the new offsets.

Nothing new: `read_new(start)` returns 0 records. `write_files()` returns None without creating a folder, and `publish(None)` is a safe no-op.

Run stops WHILE writing: `write_files()` writes the first partition (p0) to STAGING and crashes before `publish()` is ever called. LAKE remains untouched and readers see no partial data. On the next run, `write_files()` wipes the abandoned STAGING folder, and lake_position() cleanly retries from the last committed offset.

Run stops AFTER writing: All partitions finish writing to STAGING, and the crash occurs right before `publish()`. Because the files are still isolated in STAGING, LAKE has no duplicates or uncommitted records. The next run cleans STAGING, re-processes the batch, and publishes it cleanly.


# 3. output of `check_all()`

```bash
reset: lake, staging and state deleted

situation                                 in lake  duplicates  missing  not loaded    run  check
-----------------------------------------------------------------------------------------------
1 first run                             2,000,000           0        0           0  19.4s  PASS
2 new records arrive, run again         2,100,000           0        0           0   3.6s  PASS
3 nothing new, run again                2,100,000           0        0           0   0.1s  PASS
4 run stops WHILE writing: right after  2,100,000           0        0     100,000         PASS
  ... then the next run                 2,200,000           0        0           0   4.4s  PASS
5 run stops AFTER writing: right after  2,200,000           0        0     100,000         PASS
  ... then the next run                 2,300,000           0        0           0   3.5s  PASS
-----------------------------------------------------------------------------------------------
Exactly once after every run: YES

[('1 first run',
  {'topic': 2000000,
   'lake': 2000000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 4,
   'mb': 111.454807,
   'ok': True,
   'problems': []},
  19.393303632736206),
 ('2 new records arrive, run again',
  {'topic': 2100000,
   'lake': 2100000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 7,
   'mb': 112.556471,
   'ok': True,
   'problems': []},
  3.5537757873535156),
 ('3 nothing new, run again',
  {'topic': 2100000,
   'lake': 2100000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 7,
   'mb': 112.556471,
   'ok': True,
   'problems': []},
  0.08145332336425781),
 ('4 run stops WHILE writing: right after',
  {'topic': 2200000,
   'lake': 2100000,
   'dup': 0,
   'miss': 0,
   'lag': 100000,
   'files': 7,
   'mb': 112.556471,
   'ok': True,
   'problems': []},
  None),
 ('  ... then the next run',
  {'topic': 2200000,
   'lake': 2200000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 11,
   'mb': 113.710627,
   'ok': True,
   'problems': []},
  4.383565187454224),
 ('5 run stops AFTER writing: right after',
  {'topic': 2300000,
   'lake': 2200000,
   'dup': 0,
   'miss': 0,
   'lag': 100000,
   'files': 11,
   'mb': 113.710627,
   'ok': True,
   'problems': []},
  None),
 ('  ... then the next run',
  {'topic': 2300000,
   'lake': 2300000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 14,
   'mb': 114.813484,
   'ok': True,
   'problems': []},
  3.459547996520996)]
  ```