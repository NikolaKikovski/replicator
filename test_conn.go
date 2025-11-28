package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	connStr := "postgres://postgres:password@127.0.0.1:5432/source_db?replication=database&sslmode=disable"
	
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)
	
	fmt.Println("✅ Successfully connected!")
	fmt.Printf("Server version: %s\n", conn.ParameterStatus("server_version"))
}
