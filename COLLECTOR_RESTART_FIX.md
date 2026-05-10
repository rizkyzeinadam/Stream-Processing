# Flink REST Metrics Collector - Job Restart Fix v2

## Problem (Initial Issue)
When Flink job restarts after fault injection:
- Job ID changes
- Collector kept querying old job ID
- Result: continuous 404 errors  
- Task distribution CSV would stop updating

## Root Cause Analysis
Initial fix had two issues:
1. `get_running_job()` had fallbacks that could return non-RUNNING jobs
2. Only 1 retry attempt with 1-second backoff was insufficient during startup chaos

## Enhanced Solution (v2)

### 1. Strict RUNNING State Filtering
**Updated `get_running_job()`**:
- Now explicitly checks `state == 'RUNNING'` with no fallbacks
- Returns `None` if no RUNNING jobs exist (critical!)
- Logs other job states for debugging: `No RUNNING jobs found. Other jobs: [job1(FAILING), job2(FINISHED)]`
- Ensures `get_latest_job_id()` never returns a dead/restarting job

### 2. Smart Multi-Stage Recovery
**Improved `collect_once()` retry logic**:
```
Attempt 1:
  [1s wait] → Refresh job ID → Check if changed → Retry vertices

Attempt 2:
  [2s wait] → Refresh job ID → Check if changed → Retry vertices

Attempt 3:
  [3s wait] → Refresh job ID → Check if changed → Retry vertices

Result:
  Success → Proceed with collection
  All failed → Return empty (skip this cycle, try next)
```

### 3. Exponential Backoff (1s, 2s, 3s)
- Gives Flink REST API time to stabilize after restart
- Better spacing for concurrent recovery attempts
- Prevents thundering herd of immediate retries

### 4. Job ID Change Detection
- Explicitly checks if job ID changed: `if new_job_id == job_id: continue`
- Prevents infinite retry on same dead job ID
- Logs transition: `New job found! {old} → {new}`

### 5. Enhanced Logging
Critical recovery logs added:
- `Recovery attempt 1/3: waiting 1s before refresh...`
- `Job ID unchanged ({job_id}), still 404` - shows stuck state
- `New job found! {old_id} → {new_id}` - confirms successful transition
- `Failed to recover after 3 attempts, giving up on this cycle` - escape clause

## Key Changes Summary

| Component | Change | Purpose |
|-----------|--------|---------|
| `get_running_job()` | Strict RUNNING filter, no fallbacks | Never return dead jobs |
| `collect_once()` | 3 retry attempts instead of 1 | Handle slow restart scenarios |
| Retry backoff | Exponential: 1s, 2s, 3s | Give REST API stabilization time |
| Job ID check | `if new_job_id == job_id: continue` | Detect stuck state |
| Logging | Detailed recovery attempt logs | Visibility into restart flow |

## Expected Behavior During Fault Injection

### Scenario 1: Quick Job Restart
```
13:23:42.607 - 404 on old_job_id
13:23:42.608 - Recovery attempt 1: waiting 1s...
13:23:43.608 - Refresh job ID → Gets NEW_JOB_ID (changed!)
13:23:43.609 - Query new job → SUCCESS
13:23:43.610 - Continue collection with NEW_JOB_ID
```

### Scenario 2: Slow Job Restart (need multiple attempts)
```
13:23:42.607 - 404 on old_job_id
13:23:42.608 - Recovery attempt 1: waiting 1s...
13:23:43.608 - Refresh → Still no RUNNING jobs
13:23:43.609 - Recovery attempt 2: waiting 2s...
13:23:45.609 - Refresh → Gets NEW_JOB_ID
13:23:45.610 - Query new job → SUCCESS
13:23:45.611 - Continue collection
```

### Scenario 3: Job Stuck in Restart (give up gracefully)
```
13:23:42.607 - 404 on old_job_id
13:23:42.608 - Recovery attempt 1: waiting 1s...
13:23:43.608 - Refresh → No RUNNING jobs found
13:23:43.609 - Recovery attempt 2: waiting 2s...
13:23:45.609 - Refresh → Still no RUNNING jobs
13:23:45.610 - Recovery attempt 3: waiting 3s...
13:23:48.610 - Refresh → Still no job
13:23:48.611 - ERROR: Failed to recover after 3 attempts
13:23:48.612 - (Skip this cycle, wait for next interval [1s], try again)
```

## Testing the Fix

After deploying the updated collector, you should see patterns like:

**In logs** (`/tmp/task_distribution_collector.log`):
- ✓ `Found preferred RUNNING job: abc123def456`
- ✓ `No RUNNING jobs found. Other jobs: [...]` (shows restart detection)
- ✓ `Recovery attempt X/3: waiting Xs before refresh...`
- ✓ `New job found! xyz → abc123` (successful transition)
- ✓ Clean switchover with no cascading 404s

**In CSV** (`metrics/task_distribution.csv`):
- Continuous rows through fault injection moment
- No 1-minute gaps (previous behavior)
- Subtask assignment captured during transition
- Clean before/after state visible

## Comparison: Before vs After

**Before (v1):**
```
13:23:42 - 404 
13:23:42 - Retry immediately (1s backoff)
13:23:43 - Still 404 (because get_latest_job_id() wrong)
13:23:44 - Still 404 (stuck loop)
13:23:45 - Still 404 (stuck loop)
... (repeats for minutes)
```

**After (v2):**
```
13:23:42 - 404
13:23:42 - Attempt 1: wait 1s, refresh → no jobs found
13:23:43 - Attempt 2: wait 2s, refresh → NEW JOB FOUND!
13:23:45 - SUCCESS, resume collection with new job_id
13:23:46 - (Normal operation resumes)
```

## Rollout Guidance
- No configuration changes needed
- Existing `run_experiment.sh` integration unchanged
- Drop-in replacement for old collector script
- Backward compatible with existing CSV format
- Improved reliability with no performance penalty

