package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type TaskFunc func(payload map[string]any) (map[string]any, error)

var taskRegistry = map[string]TaskFunc{
	"train_model": trainModel,
	"sleep_task":  sleepTask,
	"flaky_task":  flakyTask,
}

func trainModel(payload map[string]any) (map[string]any, error) {
	dataset, _ := payload["dataset"].(string)
	if dataset == "" {
		dataset = "unknown"
	}
	time.Sleep(randDuration(1, 3))
	if rand.Float64() < 0.3 {
		return nil, fmt.Errorf("simulated failure training on %s", dataset)
	}
	return map[string]any{
		"dataset":  dataset,
		"accuracy": 0.80 + rand.Float64()*0.19,
	}, nil
}

func sleepTask(payload map[string]any) (map[string]any, error) {
	seconds := 1.0
	if s, ok := payload["seconds"].(float64); ok {
		seconds = s
	}
	time.Sleep(time.Duration(seconds * float64(time.Second)))
	return map[string]any{"slept": seconds}, nil
}

func flakyTask(payload map[string]any) (map[string]any, error) {
	failRate := 0.6
	if fr, ok := payload["fail_rate"].(float64); ok {
		failRate = fr
	}
	time.Sleep(500 * time.Millisecond)
	if rand.Float64() < failRate {
		return nil, fmt.Errorf("flaky_task intentional failure")
	}
	return map[string]any{"ok": true}, nil
}

func randDuration(minSec, maxSec float64) time.Duration {
	s := minSec + rand.Float64()*(maxSec-minSec)
	return time.Duration(s * float64(time.Second))
}

func decodePayload(raw json.RawMessage) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if m == nil {
		m = map[string]any{}
	}
	return m
}
