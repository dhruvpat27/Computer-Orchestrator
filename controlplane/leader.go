package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const leaderLockID = 727111

var isLeader atomic.Bool

func runLeaderElection(ctx context.Context, pool *pgxpool.Pool, replicaID string) {
	for {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			log.Printf("[election] failed to acquire connection: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		var acquired bool
		err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", leaderLockID).Scan(&acquired)
		if err != nil {
			log.Printf("[election] lock query failed: %v", err)
			conn.Release()
			time.Sleep(3 * time.Second)
			continue
		}

		if !acquired {
			conn.Release()
			time.Sleep(3 * time.Second)
			continue
		}

		isLeader.Store(true)
		log.Printf("[election] %s ACQUIRED LEADERSHIP", replicaID)

		go retryScheduler(ctx)
		go dagScheduler(ctx)

		waitForConnDeath(ctx, conn)

		isLeader.Store(false)
		log.Printf("[election] %s LOST LEADERSHIP (connection dropped)", replicaID)
		conn.Release()
	}
}

func waitForConnDeath(ctx context.Context, conn *pgxpool.Conn) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if err := conn.Ping(ctx); err != nil {
			return
		}
	}
}
