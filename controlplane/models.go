package main

import (
	"encoding/json"
	"time"
)

type Job struct {
	ID          string          `json:"id"`
	TaskName    string          `json:"task_name"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	RetriesLeft int             `json:"retries_left"`
	MaxRetries  int             `json:"max_retries"`
	Attempt     int             `json:"attempt"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       *string         `json:"error,omitempty"`
	WorkerID    *string         `json:"worker_id,omitempty"`
	WorkflowID  *string         `json:"workflow_id,omitempty"`
	DependsOn   json.RawMessage `json:"depends_on,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type SubmitJobRequest struct {
	TaskName   string          `json:"task_name"`
	Payload    json.RawMessage `json:"payload"`
	MaxRetries int             `json:"max_retries"`
}

// TaskSpec is one node in a submitted workflow DAG. LocalID is scoped to
// this submission only - the caller invents short names ("fetch_data",
// "train") and references them in DependsOn; the server resolves these to
// real job UUIDs before anything is inserted.
type TaskSpec struct {
	LocalID    string          `json:"id"`
	TaskName   string          `json:"task_name"`
	Payload    json.RawMessage `json:"payload"`
	DependsOn  []string        `json:"depends_on"`
	MaxRetries int             `json:"max_retries"`
}

type WorkflowSubmitRequest struct {
	Name  string     `json:"name"`
	Tasks []TaskSpec `json:"tasks"`
}

type WorkflowTaskResult struct {
	LocalID string `json:"local_id"`
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
}

type WorkflowSubmitResponse struct {
	WorkflowID string               `json:"workflow_id"`
	Tasks      []WorkflowTaskResult `json:"tasks"`
}

type StartRequest struct {
	WorkerID string `json:"worker_id"`
}

type ReportRequest struct {
	Success bool            `json:"success"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// QueueMessage is what gets pushed onto the Redis list. Workers only need
// enough to execute - Postgres remains the source of truth for full state.
type QueueMessage struct {
	JobID    string          `json:"job_id"`
	TaskName string          `json:"task_name"`
	Payload  json.RawMessage `json:"payload"`
}
