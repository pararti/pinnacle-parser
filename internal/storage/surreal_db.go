package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/pararti/pinnacle-parser/internal/models/parsed"
	"github.com/pararti/pinnacle-parser/pkg/logger"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// SurrealDBClient handles the connection and operations with SurrealDB
type SurrealDBClient struct {
	db     *surrealdb.DB
	logger *logger.Logger
	ctx    context.Context
}

// NewSurrealDBClient creates a new SurrealDB client
func NewSurrealDBClient(address, username, password, namespace, database string, logger *logger.Logger) (*SurrealDBClient, error) {
	// Create a context with timeout for initial operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Connect to SurrealDB
	db, err := surrealdb.New(address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
	}

	// Sign in to SurrealDB
	auth := &surrealdb.Auth{
		Username: username,
		Password: password,
	}

	_, err = db.WithContext(ctx).SignIn(auth)
	if err != nil {
		return nil, fmt.Errorf("failed to sign in to SurrealDB: %w", err)
	}

	// Use the specified namespace and database
	err = db.WithContext(ctx).Use(namespace, database)
	if err != nil {
		return nil, fmt.Errorf("failed to use namespace and database: %w", err)
	}

	client := &SurrealDBClient{
		db:     db,
		logger: logger,
		ctx:    context.Background(), // Use a background context for subsequent operations
	}

	// Ensure the sports table exists
	if err := client.ensureSportsTable(); err != nil {
		return nil, fmt.Errorf("failed to ensure sports table: %w", err)
	}

	return client, nil
}

// ensureSportsTable ensures that the sports table exists with the correct schema
func (s *SurrealDBClient) ensureSportsTable() error {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Use structured query instead of raw string format
	query := `INFO FOR TABLE sports`

	// Check if table exists - we don't need the result, just need to know if query succeeds
	_, err := surrealdb.Query[map[string]interface{}](s.db.WithContext(ctx), query, nil)
	if err != nil {
		s.logger.Info("Sports table not found, creating it...")

		// Create a schema with proper constraints
		createQuery := `
		DEFINE TABLE sports SCHEMAFULL;
		DEFINE FIELD id ON sports TYPE number;
		DEFINE FIELD name ON sports TYPE string;
		DEFINE INDEX sports_id ON TABLE sports COLUMNS id UNIQUE;
		DEFINE INDEX sports_name ON TABLE sports COLUMNS name UNIQUE;
		`

		// Execute the creation query with proper error handling
		_, err := surrealdb.Query[interface{}](s.db.WithContext(ctx), createQuery, nil)
		if err != nil {
			return fmt.Errorf("failed to create sports table: %w", err)
		}

		s.logger.Info("Sports table created successfully")
	}

	return nil
}

// SportRecord represents a sport record in SurrealDB
type SportRecord struct {
	ID   models.RecordID `json:"id"`
	Name string          `json:"name"`
}

// QueryResult represents a single query result
type QueryResult[T any] struct {
	Status string `json:"status"`
	Time   string `json:"time"`
	Result T      `json:"result"`
}

// StoreSport stores a sport in SurrealDB, ensuring it's unique by ID and name
// It uses RecordID to identify the specific record and Upsert to create or update it
func (s *SurrealDBClient) StoreSport(sport *parsed.Sport) error {
	if sport == nil {
		return fmt.Errorf("cannot store nil sport")
	}

	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Debug the sport data
	s.logger.Info(fmt.Sprintf("Attempting to store sport: ID=%d, Name=%s", sport.ID, sport.Name))

	// Create record ID for the sport using the models.NewRecordID helper
	// This creates a proper SurrealDB record identifier in the format "sports:<id>"
	recordID := models.NewRecordID("sports", sport.ID)
	// Create a SportRecord from the parsed.Sport
	sportRecord := SportRecord{
		ID:   recordID,
		Name: sport.Name,
	}
	// Use the Upsert[T] method with record ID
	// This will create the record if it doesn't exist, or update it if it does
	_, err := surrealdb.Upsert[SportRecord](s.db.WithContext(ctx), recordID, sportRecord)
	if err != nil {
		s.logger.Info(fmt.Sprintf("Error storing sport: %v", err))
		return fmt.Errorf("failed to store sport: %w", err)
	}

	s.logger.Info(fmt.Sprintf("Successfully stored sport: ID=%d, Name=%s", sport.ID, sport.Name))
	return nil
}

// Close closes the connection to SurrealDB
func (s *SurrealDBClient) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
