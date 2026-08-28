package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var dockerClient = &http.Client{
	Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", "/var/run/docker.sock")
		},
	},
	Timeout: 5 * time.Second,
}

type dockerContainer struct {
	ID     string   `json:"Id"`
	Names  []string `json:"Names"`
	State  string   `json:"State"`
	Status string   `json:"Status"`
}

func dockerListContainers(ctx context.Context, nameContains string) ([]dockerContainer, error) {
	filters := fmt.Sprintf(`{"name":["%s"]}`, nameContains)
	u := "http://unix/containers/json?all=true&filters=" + url.QueryEscape(filters)

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := dockerClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("docker API error %d: %s", resp.StatusCode, body)
	}

	var containers []dockerContainer
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return nil, err
	}
	return containers, nil
}

func dockerKillContainer(ctx context.Context, nameOrID string) error {
	u := fmt.Sprintf("http://unix/containers/%s/kill", nameOrID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	resp, err := dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("docker API error %d: %s", resp.StatusCode, body)
	}
	return nil
}

func containerDisplayName(c dockerContainer) string {
	if len(c.Names) == 0 {
		return c.ID[:12]
	}
	return strings.TrimPrefix(c.Names[0], "/")
}

func handleListWorkers(w http.ResponseWriter, r *http.Request) {
func handleListWorkers(w http.ResponseWriter, r *http.Request) {
	containers, err := dockerListContainers(r.Context(), "worker")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]map[string]string, 0, len(containers))
	for _, c := range containers {
		out = append(out, map[string]string{
			"name":   containerDisplayName(c),
			"state":  c.State,
			"status": c.Status,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

type chaosKillRequest struct {
	Target string `json:"target"`
}

func handleChaosKill(w http.ResponseWriter, r *http.Request) {
	var req chaosKillRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	target := req.Target
	if target == "random-worker" {
		containers, err := dockerListContainers(r.Context(), "worker")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var running []dockerContainer
		for _, c := range containers {
			if c.State == "running" {
				running = append(running, c)
			}
		}
		if len(running) == 0 {
			http.Error(w, "no running workers found", http.StatusNotFound)
			return
		}
		target = containerDisplayName(running[rand.Intn(len(running))])
	}

	if err := dockerKillContainer(r.Context(), target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"killed": target})
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	depth, err := rdb.LLen(r.Context(), queueKey).Result()
	if err != nil {
		http.Error(w, "failed to read queue depth", http.StatusInternalServerError)
		return
	}

	rows, err := pool.Query(r.Context(), `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		http.Error(w, "failed to read job counts", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	counts := map[string]int64{}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			continue
		}
		counts[status] = count
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"queue_depth": depth,
		"job_counts":  counts,
	})
}
