package storage

import (
	"context"
	"fmt"

	"github.com/pararti/pinnacle-parser/internal/models/parsed"
	"github.com/pararti/pinnacle-parser/pkg/logger"
	"github.com/surrealdb/surrealdb.go"
)

// SurrealDBClient handles the connection and operations with SurrealDB
type SurrealDBClient struct {
	db     *surrealdb.DB
	logger *logger.Logger
	ctx    context.Context
}

// NewSurrealDBClient creates a new SurrealDB client
func NewSurrealDBClient(address, username, password, namespace, database string, logger *logger.Logger) (*SurrealDBClient, error) {
	ctx := context.Background()

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

	_, err = db.SignIn(auth)
	if err != nil {
		return nil, fmt.Errorf("failed to sign in to SurrealDB: %w", err)
	}

	// Use the specified namespace and database
	err = db.Use(namespace, database)
	if err != nil {
		return nil, fmt.Errorf("failed to use namespace and database: %w", err)
	}

	client := &SurrealDBClient{
		db:     db,
		logger: logger,
		ctx:    ctx,
	}

	// Ensure the sports table exists
	if err := client.ensureSportsTable(); err != nil {
		return nil, fmt.Errorf("failed to ensure sports table: %w", err)
	}

	return client, nil
}

// ensureSportsTable ensures that the sports table exists with the correct schema
func (s *SurrealDBClient) ensureSportsTable() error {
	// Check if the sports table exists by querying for its definition
	query := "INFO FOR TABLE sports"

	// Execute the query directly using Send method to avoid type issues
	var result interface{}
	err := s.db.Send(&result, "query", query, nil)

	// If there's an error, the table might not exist, so we create it
	if err != nil {
		s.logger.Info("Sports table not found, creating it...")

		// Create the sports table with a schema that enforces uniqueness on id and name
		createQuery := `
		DEFINE TABLE sports SCHEMAFULL;
		DEFINE FIELD id ON sports TYPE number;
		DEFINE FIELD name ON sports TYPE string;
		DEFINE INDEX sports_id ON TABLE sports COLUMNS id UNIQUE;
		DEFINE INDEX sports_name ON TABLE sports COLUMNS name UNIQUE;
		`

		// Execute the creation query directly
		var createResult interface{}
		err := s.db.Send(&createResult, "query", createQuery, nil)
		if err != nil {
			return fmt.Errorf("failed to create sports table: %w", err)
		}

		s.logger.Info("Sports table created successfully")
	}

	return nil
}

// SportRecord represents a sport record in SurrealDB
type SportRecord struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// QueryResponse represents a response from SurrealDB
type QueryResponse struct {
	Status string      `json:"status"`
	Time   string      `json:"time"`
	Result interface{} `json:"result"`
}

// StoreSport stores a sport in SurrealDB, ensuring it's unique by ID and name
func (s *SurrealDBClient) StoreSport(sport *parsed.Sport) error {
	if sport == nil {
		return fmt.Errorf("cannot store nil sport")
	}

	// Check if sport already exists by ID
	query := fmt.Sprintf("SELECT * FROM sports WHERE id = %d", sport.ID)

	// Use any for the result type to safely handle whatever is returned
	var result interface{}
	err := s.db.Send(&result, "query", query, nil)
	if err != nil {
		return fmt.Errorf("failed to query sport by ID: %w", err)
	}

	// Check if we got any results by examining the result structure
	// The result is an array of query responses, and we need to check if any records were returned
	results, ok := result.([]interface{})
	hasRecords := false

	if ok && len(results) > 0 {
		// Check if the first result has any records
		if firstResult, ok := results[0].(map[string]interface{}); ok {
			if resultArray, ok := firstResult["result"].([]interface{}); ok {
				hasRecords = len(resultArray) > 0
			}
		}
	}

	if hasRecords {
		s.logger.Info(fmt.Sprintf("Sport already exists: ID=%d, Name=%s", sport.ID, sport.Name))
		return nil
	}

	// Sport doesn't exist, create it
	s.logger.Info(fmt.Sprintf("Adding new sport: ID=%d, Name=%s", sport.ID, sport.Name))

	// Create the sport record using a simple insert statement to avoid type issues
	createQuery := fmt.Sprintf("INSERT INTO sports (id, name) VALUES (%d, '%s')", sport.ID, sport.Name)

	var createResult interface{}
	err = s.db.Send(&createResult, "query", createQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to create sport: %w", err)
	}

	return nil
}

// Close closes the connection to SurrealDB
func (s *SurrealDBClient) Close() {
	if s.db != nil {
		s.db.Close()
	}
}
