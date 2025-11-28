package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

func main() {
	// Test regular connection first
	connStr := "postgres://postgres:password@127.0.0.1:5432/source_db?sslmode=disable"
	
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatalf("Failed to connect (regular): %v", err)
	}
	defer conn.Close(ctx)
	
	var dbname string
	err = conn.QueryRow(ctx, "SELECT current_database()").Scan(&dbname)
	if err != nil {
		log.Fatalf("Query failed: %v", err) 
	}
	
	fmt.Printf("✅ Connected to database: %s\n", dbname)
}
