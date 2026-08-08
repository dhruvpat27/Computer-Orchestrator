package main

import (
	"context"
	"encoding/json"
	"log"
	"time"
)

const retryPollInterval = 2 * time.Second

// backoffSeconds implements exponential backoff: attempt 1 -> 1s, 2 -> 2s, 3 -> 4s ...
func backoffSeconds(attempt int) int {
	if attempt < 1 {
		attempt = 1
	}
	return 1 << (attempt - 1)
}

// retryScheduler is the retry brain. Every couple seconds it looks for jobs
// sitting in FAILED_RETRY_PENDING whose backoff window has elapsed, flips
// them back to QUEUED, and pushes them back onto the Redis queue. This is
// what turns a failure into real retry semantics instead of "oh well."
//
// Only the elected leader runs this. It checks isLeader on every tick and
// exits cleanly the moment leadership is lost - runLeaderElection starts a
// fresh one if/when this replica becomes leader again.
func retryScheduler(ctx context.Context) {
	ticker := time.NewTicker(retryPollInterval)
	defer ticker.Stop()

	for range ticker.C {
		if !isLeader.Load() {
			log.Println("[retry-scheduler] no longer leader, stopping")
			return
		}

		rows, err := pool.Query(ctx, `
			UPDATE jobs
			SET status = 'QUEUED', updated_at = now()
			WHERE status = 'FAILED_RETRY_PENDING'
			  AND next_retry_at <= now()
			RETURNING id, task_name, payload
		`)
		if err != nil {
			log.Printf("retry scheduler query error: %v", err)
			continue
		}

		for rows.Next() {
			var id, taskName string
			var payload json.RawMessage
			if err := rows.Scan(&id, &taskName, &payload); err != nil {
				log.Printf("retry scheduler scan error: %v", err)
				continue
			}

			msg := QueueMessage{JobID: id, TaskName: taskName, Payload: payload}
			data, _ := json.Marshal(msg)
			if err := rdb.LPush(ctx, queueKey, data).Err(); err != nil {
				log.Printf("failed to requeue job %s: %v", id, err)
				continue
			}
			log.Printf("requeued job %s for retry", id)
		}
		rows.Close()
	}
}
