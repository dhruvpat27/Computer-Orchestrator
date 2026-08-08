package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const queueKey = "job_queue"

var (
	rdb           *redis.Client
	controlPlanes []string
	workerID      string
	httpClient    = &http.Client{Timeout: 3 * time.Second}
)

type QueueMessage struct {
	JobID    string          `json:"job_id"`
	TaskName string          `json:"task_name"`
	Payload  json.RawMessage `json:"payload"`
}

func main() {
	redisURL := mustEnv("REDIS_URL")
	rawURLs := os.Getenv("CONTROL_PLANE_URLS")
	if rawURLs == "" {
		rawURLs = "http://controlplane-1:8000,http://controlplane-2:8000,http://controlplane-3:8000"
	}
	controlPlanes = strings.Split(rawURLs, ",")

	hostname, _ := os.Hostname()
	workerID = fmt.Sprintf("%s-%s", hostname, uuid.NewString()[:8])

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("invalid REDIS_URL: %v", err)
	}
	rdb = redis.NewClient(opt)
	ctx := context.Background()

	time.Sleep(3 * time.Second) // crude wait for control plane to come up - fine for a weekend project
	log.Printf("worker %s starting, polling '%s'", workerID, queueKey)

	for {
		result, err := rdb.BRPop(ctx, 5*time.Second, queueKey).Result()
		if err == redis.Nil {
			continue // nothing queued, poll again
		}
		if err != nil {
			log.Printf("redis BRPOP error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// BRPop returns [key, value]
		raw := result[1]
		var msg QueueMessage
		if err := json.Unmarshal([]byte(raw), &msg); err != nil {
			log.Printf("bad queue message: %v", err)
			continue
		}

		handleJob(ctx, msg)
	}
}

func handleJob(ctx context.Context, msg QueueMessage) {
	log.Printf("[%s] picked up job %s (%s)", workerID, msg.JobID, msg.TaskName)

	if err := reportStart(msg.JobID); err != nil {
		log.Printf("failed to report start for job %s: %v", msg.JobID, err)
	}

	taskFn, ok := taskRegistry[msg.TaskName]
	if !ok {
		reportResult(msg.JobID, false, nil, fmt.Sprintf("unknown task '%s'", msg.TaskName))
		return
	}

	result, err := taskFn(decodePayload(msg.Payload))
	if err != nil {
		log.Printf("[%s] job %s FAILED: %v", workerID, msg.JobID, err)
		reportResult(msg.JobID, false, nil, err.Error())
		return
	}

	log.Printf("[%s] job %s SUCCESS", workerID, msg.JobID)
	reportResult(msg.JobID, true, result, "")
}

func reportStart(jobID string) error {
	body, _ := json.Marshal(map[string]string{"worker_id": workerID})
	resp, err := postWithFailover(fmt.Sprintf("/jobs/%s/start", jobID), body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func reportResult(jobID string, success bool, result map[string]any, errMsg string) {
	payload := map[string]any{"success": success}
	if result != nil {
		payload["result"] = result
	}
	if errMsg != "" {
		payload["error"] = errMsg
	}
	body, _ := json.Marshal(payload)

	resp, err := postWithFailover(fmt.Sprintf("/jobs/%s/report", jobID), body)
	if err != nil {
		log.Printf("failed to report result for job %s: %v", jobID, err)
		return
	}
	defer resp.Body.Close()
}

// postWithFailover tries each known control-plane replica in order and
// returns on the first one that answers. This is what lets a worker keep
// reporting job results even while one replica is down or mid-election -
// no single control-plane instance is a hard dependency for the worker.
func postWithFailover(path string, body []byte) (*http.Response, error) {
	var lastErr error
	for _, base := range controlPlanes {
		resp, err := httpClient.Post(base+path, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("%s returned %d", base, resp.StatusCode)
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("all control plane replicas unreachable: %v", lastErr)
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return v
}
