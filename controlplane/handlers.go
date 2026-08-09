package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleLeaderStatus lets you (or a dashboard) ask any replica "are you in
// charge right now?" - this is what makes leadership visible from outside.
func handleLeaderStatus(replicaID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"replica_id": replicaID,
			"is_leader":  isLeader.Load(),
		})
	}
}

func handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req SubmitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if req.TaskName == "" {
		http.Error(w, "task_name is required", http.StatusBadRequest)
		return
	}
	if req.MaxRetries <= 0 {
		req.MaxRetries = 3
	}
	if req.Payload == nil {
		req.Payload = json.RawMessage(`{}`)
	}

	row := pool.QueryRow(ctx, `
		INSERT INTO jobs (task_name, payload, status, retries_left, max_retries)
		VALUES ($1, $2, 'QUEUED', $3, $3)
		RETURNING id, task_name, payload, status, retries_left, max_retries, attempt,
		          result, error, worker_id, workflow_id, depends_on, created_at, updated_at
	`, req.TaskName, req.Payload, req.MaxRetries)

	job, err := scanJob(row)
	if err != nil {
		log.Printf("insert job failed: %v", err)
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	msg := QueueMessage{JobID: job.ID, TaskName: job.TaskName, Payload: job.Payload}
	payload, _ := json.Marshal(msg)
	if err := rdb.LPush(ctx, queueKey, payload).Err(); err != nil {
		log.Printf("failed to enqueue job %s: %v", job.ID, err)
		http.Error(w, "job created but failed to enqueue", http.StatusInternalServerError)
		return
	}

	log.Printf("submitted job %s (%s)", job.ID, job.TaskName)
	writeJSON(w, http.StatusCreated, job)
}

func handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	row := pool.QueryRow(ctx, `
		SELECT id, task_name, payload, status, retries_left, max_retries, attempt,
		       result, error, worker_id, workflow_id, depends_on, created_at, updated_at
		FROM jobs WHERE id = $1
	`, id)

	job, err := scanJob(row)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func handleListJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	workflowID := r.URL.Query().Get("workflow_id")

	var rows pgx.Rows
	var err error
	switch {
	case workflowID != "":
		rows, err = pool.Query(ctx, `
			SELECT id, task_name, payload, status, retries_left, max_retries, attempt,
			       result, error, worker_id, workflow_id, depends_on, created_at, updated_at
			FROM jobs WHERE workflow_id = $1 ORDER BY created_at ASC
		`, workflowID)
	case status != "":
		rows, err = pool.Query(ctx, `
			SELECT id, task_name, payload, status, retries_left, max_retries, attempt,
			       result, error, worker_id, workflow_id, depends_on, created_at, updated_at
			FROM jobs WHERE status = $1 ORDER BY created_at DESC LIMIT 100
		`, status)
	default:
		rows, err = pool.Query(ctx, `
			SELECT id, task_name, payload, status, retries_left, max_retries, attempt,
			       result, error, worker_id, workflow_id, depends_on, created_at, updated_at
			FROM jobs ORDER BY created_at DESC LIMIT 100
		`)
	}
	if err != nil {
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.TaskName, &j.Payload, &j.Status, &j.RetriesLeft,
			&j.MaxRetries, &j.Attempt, &j.Result, &j.Error, &j.WorkerID,
			&j.WorkflowID, &j.DependsOn, &j.CreatedAt, &j.UpdatedAt); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	writeJSON(w, http.StatusOK, jobs)
}

func handleStartJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	tag, err := pool.Exec(ctx, `
		UPDATE jobs SET status = 'RUNNING', worker_id = $2, updated_at = now() WHERE id = $1
	`, id, req.WorkerID)
	if err != nil || tag.RowsAffected() == 0 {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"acknowledged": true})
}

// handleReportJob is the only write path workers have into job state - keeps
// the control plane as the single source of truth and workers as dumb,
// stateless executors. This is also where retry decisions get made.
func handleReportJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req ReportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	var retriesLeft, attempt int
	err := pool.QueryRow(ctx, `SELECT retries_left, attempt FROM jobs WHERE id = $1`, id).
		Scan(&retriesLeft, &attempt)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if req.Success {
		result := req.Result
		if result == nil {
			result = json.RawMessage(`{}`)
		}
		_, err = pool.Exec(ctx, `
			UPDATE jobs SET status = 'SUCCESS', result = $2, error = NULL, updated_at = now()
			WHERE id = $1
		`, id, result)
		if err != nil {
			http.Error(w, "update failed", http.StatusInternalServerError)
			return
		}
		log.Printf("job %s SUCCESS", id)
	} else {
		retriesLeft--
		attempt++
		errMsg := req.Error
		if errMsg == "" {
			errMsg = "unknown error"
		}

		if retriesLeft > 0 {
			delay := backoffSeconds(attempt)
			nextRetryAt := time.Now().Add(time.Duration(delay) * time.Second)
			_, err = pool.Exec(ctx, `
				UPDATE jobs
				SET status = 'FAILED_RETRY_PENDING', retries_left = $2, attempt = $3,
				    error = $4, next_retry_at = $5, updated_at = now()
				WHERE id = $1
			`, id, retriesLeft, attempt, errMsg, nextRetryAt)
			log.Printf("job %s failed, retrying in %ds (%d retries left)", id, delay, retriesLeft)
		} else {
			_, err = pool.Exec(ctx, `
				UPDATE jobs
				SET status = 'FAILED', retries_left = $2, attempt = $3, error = $4, updated_at = now()
				WHERE id = $1
			`, id, retriesLeft, attempt, errMsg)
			log.Printf("job %s exhausted retries, marking FAILED", id)
		}
		if err != nil {
			http.Error(w, "update failed", http.StatusInternalServerError)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]bool{"acknowledged": true})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
