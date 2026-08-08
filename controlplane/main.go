package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const queueKey = "job_queue"

var (
	pool *pgxpool.Pool
	rdb  *redis.Client
	ctx  = context.Background()
)

func main() {
	dbURL := mustEnv("DATABASE_URL")
	redisURL := mustEnv("REDIS_URL")

	var err error
	pool, err = pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("invalid REDIS_URL: %v", err)
	}
	rdb = redis.NewClient(opt)
	defer rdb.Close()

	// Leader election runs forever in the background. It grants this replica
	// the retry-scheduler job only when it holds the Postgres advisory lock.
	replicaID := os.Getenv("REPLICA_ID")
	if replicaID == "" {
		replicaID = "unknown-replica"
	}
	go runLeaderElection(ctx, pool, replicaID)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /leader", handleLeaderStatus(replicaID))
	mux.HandleFunc("POST /jobs", handleSubmitJob)
	mux.HandleFunc("GET /jobs", handleListJobs)
	mux.HandleFunc("GET /jobs/{id}", handleGetJob)
	mux.HandleFunc("POST /jobs/{id}/start", handleStartJob)
	mux.HandleFunc("POST /jobs/{id}/report", handleReportJob)

	addr := ":8000"
	log.Printf("control plane listening on %s", addr)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return v
}
