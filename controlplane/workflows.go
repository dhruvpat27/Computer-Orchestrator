package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/google/uuid"
)

// handleSubmitWorkflow accepts a whole DAG of tasks in one call, validates
// it's actually acyclic, resolves the caller's short-lived local task IDs
// into real job UUIDs, and inserts every job atomically in a single
// transaction. Tasks with no dependencies go straight to QUEUED; everything
// else starts BLOCKED and waits for dagScheduler to promote or skip it.
func handleSubmitWorkflow(w http.ResponseWriter, r *http.Request) {
	var req WorkflowSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if len(req.Tasks) == 0 {
		http.Error(w, "at least one task is required", http.StatusBadRequest)
		return
	}

	// Validate local IDs are unique and every depends_on reference is known.
	seen := map[string]bool{}
	for _, t := range req.Tasks {
		if t.LocalID == "" {
			http.Error(w, "every task needs a non-empty \"id\"", http.StatusBadRequest)
			return
		}
		if seen[t.LocalID] {
			http.Error(w, fmt.Sprintf("duplicate task id %q", t.LocalID), http.StatusBadRequest)
			return
		}
		seen[t.LocalID] = true
	}
	for _, t := range req.Tasks {
		for _, dep := range t.DependsOn {
			if !seen[dep] {
				http.Error(w, fmt.Sprintf("task %q depends on unknown task %q", t.LocalID, dep), http.StatusBadRequest)
				return
			}
		}
	}

	// Reject cycles up front - a workflow that can never complete shouldn't
	// even be accepted, let alone silently sit BLOCKED forever.
	if cyclePath := findCycle(req.Tasks); cyclePath != nil {
		http.Error(w, fmt.Sprintf("dependency cycle detected: %v", cyclePath), http.StatusBadRequest)
		return
	}

	workflowID := uuid.NewString()
	localToReal := make(map[string]string, len(req.Tasks))
	for _, t := range req.Tasks {
		localToReal[t.LocalID] = uuid.NewString()
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		http.Error(w, "failed to start transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx) // no-op if committed

	results := make([]WorkflowTaskResult, 0, len(req.Tasks))
	var readyToQueue []QueueMessage

	for _, t := range req.Tasks {
		jobID := localToReal[t.LocalID]
		maxRetries := t.MaxRetries
		if maxRetries <= 0 {
			maxRetries = 3
		}
		payload := t.Payload
		if payload == nil {
			payload = json.RawMessage(`{}`)
		}

		realDeps := make([]string, len(t.DependsOn))
		for i, d := range t.DependsOn {
			realDeps[i] = localToReal[d]
		}
		dependsOnJSON, _ := json.Marshal(realDeps)

		status := "QUEUED"
		if len(realDeps) > 0 {
			status = "BLOCKED"
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO jobs (id, task_name, payload, status, retries_left, max_retries, workflow_id, depends_on)
			VALUES ($1, $2, $3, $4, $5, $5, $6, $7)
		`, jobID, t.TaskName, payload, status, maxRetries, workflowID, dependsOnJSON)
		if err != nil {
			log.Printf("workflow insert failed for task %s: %v", t.LocalID, err)
			http.Error(w, "failed to create workflow", http.StatusInternalServerError)
			return
		}

		results = append(results, WorkflowTaskResult{LocalID: t.LocalID, JobID: jobID, Status: status})
		if status == "QUEUED" {
			readyToQueue = append(readyToQueue, QueueMessage{JobID: jobID, TaskName: t.TaskName, Payload: payload})
		}
	}

	if err := tx.Commit(ctx); err != nil {
		http.Error(w, "failed to commit workflow", http.StatusInternalServerError)
		return
	}

	// Only push to Redis after the transaction commits - otherwise a worker
	// could pick up a job whose row isn't visible yet.
	for _, msg := range readyToQueue {
		data, _ := json.Marshal(msg)
		if err := rdb.LPush(ctx, queueKey, data).Err(); err != nil {
			log.Printf("failed to enqueue workflow job %s: %v", msg.JobID, err)
		}
	}

	log.Printf("submitted workflow %s (%q) with %d tasks", workflowID, req.Name, len(req.Tasks))
	writeJSON(w, http.StatusCreated, WorkflowSubmitResponse{WorkflowID: workflowID, Tasks: results})
}

// findCycle runs a DFS with recursion-stack tracking over the submitted
// task graph and returns the offending path if one exists, nil otherwise.
// Classic white/gray/black coloring: white = unvisited, gray = on the
// current DFS stack (visiting it again means a cycle), black = fully done.
func findCycle(tasks []TaskSpec) []string {
	adj := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		adj[t.LocalID] = t.DependsOn
	}

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(tasks))
	var path []string

	var visit func(node string) []string
	visit = func(node string) []string {
		color[node] = gray
		path = append(path, node)

		for _, dep := range adj[node] {
			switch color[dep] {
			case gray:
				return append(append([]string{}, path...), dep)
			case white:
				if cyc := visit(dep); cyc != nil {
					return cyc
				}
			}
		}

		color[node] = black
		path = path[:len(path)-1]
		return nil
	}

	for _, t := range tasks {
		if color[t.LocalID] == white {
			if cyc := visit(t.LocalID); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

// handleGetWorkflow returns every job in a workflow plus a rolled-up status:
// FAILED if anything failed or was skipped, SUCCESS if everything succeeded,
// otherwise RUNNING.
func handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("id")

	rows, err := pool.Query(ctx, `
		SELECT id, task_name, payload, status, retries_left, max_retries, attempt,
		       result, error, worker_id, workflow_id, depends_on, created_at, updated_at
		FROM jobs WHERE workflow_id = $1 ORDER BY created_at ASC
	`, workflowID)
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

	if len(jobs) == 0 {
		http.Error(w, "workflow not found", http.StatusNotFound)
		return
	}

	overall := "SUCCESS"
	for _, j := range jobs {
		switch j.Status {
		case "FAILED", "SKIPPED":
			overall = "FAILED"
		case "SUCCESS":
			// no-op, stays whatever it currently is
		default:
			if overall != "FAILED" {
				overall = "RUNNING"
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"workflow_id": workflowID,
		"status":      overall,
		"jobs":        jobs,
	})
}
