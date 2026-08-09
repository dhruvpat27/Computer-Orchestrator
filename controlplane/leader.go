package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// leaderLockID is an arbitrary shared constant - every replica tries to lock
// the same number. Postgres advisory locks are just integers with no meaning
// beyond "whoever holds this number is in charge."
const leaderLockID = 727111

var isLeader atomic.Bool

// runLeaderElection continuously tries to become leader using a Postgres
// session-level advisory lock. The lock is tied to a single database
// connection - if this replica dies or its connection drops for any reason,
// Postgres releases the lock automatically and another replica picks it up
// on its next attempt. No heartbeats, no manual failure detection.
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

		// We're the leader now. Hold this connection open for as long as we
		// hold the lock - releasing it back to the pool would let Postgres
		// hand it to someone else and silently drop our lock underneath us.
		isLeader.Store(true)
		log.Printf("[election] %s ACQUIRED LEADERSHIP", replicaID)

		go retryScheduler(ctx) // start the retry brain now that we're in charge
		go dagScheduler(ctx)   // and the DAG brain - same leader-only gate

		waitForConnDeath(ctx, conn)

		isLeader.Store(false)
		log.Printf("[election] %s LOST LEADERSHIP (connection dropped)", replicaID)
		conn.Release()
	}
}

// waitForConnDeath blocks by pinging the held connection periodically,
// returning as soon as a ping fails - meaning the connection (and therefore
// our advisory lock) is gone.
func waitForConnDeath(ctx context.Context, conn *pgxpool.Conn) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if err := conn.Ping(ctx); err != nil {
			return
		}
	}
}
