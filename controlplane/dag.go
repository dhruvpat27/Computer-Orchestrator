package main

import (
	"context"
	"encoding/json"
	"log"
	"time"
)

const dagPollInterval = 2 * time.Second


func dagScheduler(ctx context.Context) {
	ticker := time.NewTicker(dagPollInterval)
	defer ticker.Stop()

	for range ticker.C {
		if !isLeader.Load() {
			log.Println("[dag-scheduler] no longer leader, stopping")
			return
		}

		skipDoomedJobs(ctx)
		promoteReadyJobs(ctx)
	}
}

func skipDoomedJobs(ctx context.Context) {
	rows, err := pool.Query(ctx, `
		UPDATE jobs j
		SET status = 'SKIPPED', error = 'upstream dependency failed', updated_at = now()
		WHERE j.status = 'BLOCKED'
		  AND EXISTS (
		    SELECT 1 FROM jsonb_array_elements_text(j.depends_on) dep_id
		    JOIN jobs d ON d.id = dep_id::uuid
		    WHERE d.status IN ('FAILED', 'SKIPPED')
		  )
		RETURNING j.id
	`)
	if err != nil {
		log.Printf("dag skip-sweep error: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			log.Printf("job %s SKIPPED (upstream dependency failed)", id)
		}
	}
}

func promoteReadyJobs(ctx context.Context) {
	rows, err := pool.Query(ctx, `
		UPDATE jobs j
		SET status = 'QUEUED', updated_at = now()
		WHERE j.status = 'BLOCKED'
		  AND NOT EXISTS (
		    SELECT 1 FROM jsonb_array_elements_text(j.depends_on) dep_id
		    LEFT JOIN jobs d ON d.id = dep_id::uuid
		    WHERE d.status IS DISTINCT FROM 'SUCCESS'
		  )
		RETURNING j.id, j.task_name, j.payload
	`)
	if err != nil {
		log.Printf("dag promote-sweep error: %v", err)
		return
	}
	defer rows.Close()

	type promoted struct {
		id, taskName string
		payload      json.RawMessage
	}
	var toQueue []promoted
	for rows.Next() {
		var p promoted
		if err := rows.Scan(&p.id, &p.taskName, &p.payload); err != nil {
			continue
		}
		toQueue = append(toQueue, p)
	}
	rows.Close()

	for _, p := range toQueue {
		msg := QueueMessage{JobID: p.id, TaskName: p.taskName, Payload: p.payload}
		data, _ := json.Marshal(msg)
		if err := rdb.LPush(ctx, queueKey, data).Err(); err != nil {
			log.Printf("failed to enqueue promoted job %s: %v", p.id, err)
			continue
		}
		log.Printf("job %s dependencies satisfied, promoted to QUEUED", p.id)
	}
}
