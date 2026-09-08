// Package db holds the Postgres wiring the services share. Every service owns
// one schema and creates it on start-up: the DDL of a schema lives with the
// service that writes it, not in a central migration directory.
package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultDSN is what docker-compose.yml exposes to the host. Port 5434
// because 5432 and 5433 were both taken on the development machine. The user
// in it is a placeholder: Open replaces it with the role of the service
// connecting, and every role shares the one password because this is a demo.
const DefaultDSN = "postgres://service:demo@localhost:5434/kafkademo?sslmode=disable"

// Open connects as role and waits for the database to answer, so a service
// fails at start-up rather than on its first event.
//
// The role, not the DSN, is what confines a service to its own schema:
// docker/postgres/init.sql gives each role exactly one schema to own.
func Open(ctx context.Context, dsn, role string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	cfg.ConnConfig.User = role

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres as %s: %w", role, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres as %s: %w", role, err)
	}
	return pool, nil
}

// Apply runs a service's DDL. Every statement in it is written to be
// idempotent, so this is safe on every start.
func Apply(ctx context.Context, pool *pgxpool.Pool, ddl string) error {
	if _, err := pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}
