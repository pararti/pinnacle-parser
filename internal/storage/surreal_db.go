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

	return client, nil
}

// SportRecord represents a sport record in SurrealDB
type SportRecord struct {
	ID   models.RecordID `json:"id"`
	Name string          `json:"name"`
}

// LeagueRecord represents a league record in SurrealDB
type LeagueRecord struct {
	ID         models.RecordID `json:"id"`
	Name       string          `json:"name"`
	Group      string          `json:"group,omitempty"`
	IsHidden   bool            `json:"isHidden"`
	IsPromoted bool            `json:"isPromoted"`
	IsSticky   bool            `json:"isSticky"`
	Sequence   int             `json:"sequence"`
}

// ParticipantRecord represents a participant record in SurrealDB with ID
type ParticipantRecord struct {
	ID        models.RecordID `json:"id,omitempty"`
	Name      string          `json:"name"`
	Alignment string          `json:"alignment,omitempty"`
}

// ParticipantData represents participant data without ID for creating new records
type ParticipantData struct {
	Name      string `json:"name"`
	Alignment string `json:"alignment,omitempty"`
}

// QueryResult represents a single query result
type QueryResult[T any] struct {
	Status string `json:"status"`
	Time   string `json:"time"`
	Result T      `json:"result"`
}

// MatchRecord represents a match record in SurrealDB
type MatchRecord struct {
	ID        models.RecordID       `json:"id"`
	BestOfX   int                   `json:"bestOfX"`
	IsLive    bool                  `json:"isLive"`
	StartTime models.CustomDateTime `json:"startTime"`
	ParentId  int                   `json:"parentId,omitempty"`
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

// StoreLeague stores a league in SurrealDB and creates a BELONGS_TO relationship to its sport
func (s *SurrealDBClient) StoreLeague(league *parsed.League) error {
	if league == nil {
		return fmt.Errorf("cannot store nil league")
	}

	if league.Sport == nil {
		return fmt.Errorf("league must have an associated sport")
	}

	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Debug the league data
	s.logger.Info(fmt.Sprintf("Attempting to store league: ID=%d, Name=%s, Sport ID=%d",
		league.ID, league.Name, league.Sport.ID))

	// Create record ID for the league
	leagueRecordID := models.NewRecordID("leagues", league.ID)

	// Create a LeagueRecord from the parsed.League (without sport reference)
	leagueRecord := LeagueRecord{
		ID:         leagueRecordID,
		Name:       league.Name,
		Group:      league.Group,
		IsHidden:   league.IsHidden,
		IsPromoted: league.IsPromoted,
		IsSticky:   league.IsSticky,
		Sequence:   league.Sequence,
	}

	// Upsert the league record
	_, err := surrealdb.Upsert[LeagueRecord](s.db.WithContext(ctx), leagueRecordID, leagueRecord)
	if err != nil {
		s.logger.Info(fmt.Sprintf("Error storing league: %v", err))
		return fmt.Errorf("failed to store league: %w", err)
	}

	// Now create a relationship between league and sport
	sportRecordID := models.NewRecordID("sports", league.Sport.ID)

	// Define the relationship - league BELONGS_TO sport
	relation := models.Table("belongs_to")

	// Create the relationship object
	relationship := &surrealdb.Relationship{
		In:       sportRecordID,  // The target node (sport)
		Out:      leagueRecordID, // The source node (league)
		Relation: relation,       // The edge type (belongs_to)
		// Optional data to store on the relationship edge
		Data: map[string]any{
			"created_at": time.Now(),
		},
	}

	// Create the relationship in SurrealDB
	err = surrealdb.Relate(s.db.WithContext(ctx), relationship)
	if err != nil {
		s.logger.Info(fmt.Sprintf("Error creating relationship between league and sport: %v", err))
		return fmt.Errorf("failed to create league-sport relationship: %w", err)
	}

	s.logger.Info(fmt.Sprintf("Successfully stored league: ID=%d, Name=%s and created relationship to sport ID=%d",
		league.ID, league.Name, league.Sport.ID))
	return nil
}

// StoreParticipants creates new participant records in SurrealDB with randomly generated IDs
// and returns the created records
func (s *SurrealDBClient) StoreParticipants(participants []*parsed.Participant) ([]*ParticipantRecord, error) {
	if participants == nil || len(participants) == 0 {
		return nil, fmt.Errorf("cannot store nil or empty participants")
	}

	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Table name for participants
	participantsTable := models.Table("participants")

	// Store the created participant records for returning
	createdRecords := make([]*ParticipantRecord, 0, len(participants))

	for _, participant := range participants {
		// Debug the participant data
		s.logger.Info(fmt.Sprintf("Attempting to store participant: Name=%s, Alignment=%s",
			participant.Name, participant.Alignment))

		// Create a ParticipantData from the parsed.Participant
		// This doesn't include an ID field which will be auto-generated by SurrealDB
		participantData := ParticipantData{
			Name:      participant.Name,
			Alignment: participant.Alignment,
		}

		// Use the Create method with the table name (not a specific record ID)
		// This will create a new record with a random ID generated by SurrealDB
		createdRecord, err := surrealdb.Create[ParticipantRecord](s.db.WithContext(ctx), participantsTable, participantData)
		if err != nil {
			s.logger.Info(fmt.Sprintf("Error storing participant: %v", err))
			return nil, fmt.Errorf("failed to store participant: %w", err)
		}

		if createdRecord != nil {
			createdRecords = append(createdRecords, createdRecord)
			s.logger.Info(fmt.Sprintf("Successfully stored participant: Name=%s, ID=%v",
				participant.Name, createdRecord.ID))
		}
	}

	return createdRecords, nil
}

// StoreMatch stores a complete match in SurrealDB, along with its relationships to league and participants
func (s *SurrealDBClient) StoreMatch(match *parsed.Match) error {
	if match == nil {
		return fmt.Errorf("cannot store nil match")
	}

	if match.League == nil {
		return fmt.Errorf("match must have an associated league")
	}

	if len(match.Participants) == 0 {
		return fmt.Errorf("match must have at least one participant")
	}

	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	// Debug the match data
	s.logger.Info(fmt.Sprintf("Attempting to store match: ID=%d, BestOfX=%d, IsLive=%t",
		match.ID, match.BestOfX, match.IsLive))

	// First, ensure sport and league are stored
	if match.League.Sport != nil {
		err := s.StoreSport(match.League.Sport)
		if err != nil {
			return fmt.Errorf("failed to store sport for match: %w", err)
		}
	}

	err := s.StoreLeague(match.League)
	if err != nil {
		return fmt.Errorf("failed to store league for match: %w", err)
	}

	// Next, store participants
	participants, err := s.StoreParticipants(match.Participants)
	if err != nil {
		return fmt.Errorf("failed to store participants for match: %w", err)
	}

	// Create record ID for the match
	matchRecordID := models.NewRecordID("matches", match.ID)

	// Convert time.Time to models.CustomDateTime
	customStartTime := models.CustomDateTime{Time: match.StartTime}

	// Create a MatchRecord from the parsed.Match
	matchRecord := MatchRecord{
		ID:        matchRecordID,
		BestOfX:   match.BestOfX,
		IsLive:    match.IsLive,
		StartTime: customStartTime,
		ParentId:  match.ParentId,
	}

	// Upsert the match record
	_, err = surrealdb.Upsert[MatchRecord](s.db.WithContext(ctx), matchRecordID, matchRecord)
	if err != nil {
		s.logger.Info(fmt.Sprintf("Error storing match: %v", err))
		return fmt.Errorf("failed to store match: %w", err)
	}

	// Create a relationship between match and league
	leagueRecordID := models.NewRecordID("leagues", match.League.ID)
	leagueRelation := &surrealdb.Relationship{
		In:       leagueRecordID,             // The target node (league)
		Out:      matchRecordID,              // The source node (match)
		Relation: models.Table("belongs_to"), // The edge type
		Data: map[string]any{
			"created_at": customStartTime,
		},
	}

	// Create the relationship using InsertRelation
	err = surrealdb.InsertRelation(s.db.WithContext(ctx), leagueRelation)
	if err != nil {
		s.logger.Info(fmt.Sprintf("Error creating relationship between match and league: %v", err))
		return fmt.Errorf("failed to create match-league relationship: %w", err)
	}

	// Create relationships between match and participants
	for i, participant := range participants {
		participantRelation := &surrealdb.Relationship{
			In:       participant.ID,                  // The target node (participant)
			Out:      matchRecordID,                   // The source node (match)
			Relation: models.Table("has_participant"), // The edge type
			Data: map[string]any{
				"created_at": customStartTime,
				"order":      i, // Store the order of participants (e.g., home/away)
				"alignment":  match.Participants[i].Alignment,
			},
		}

		// Create the relationship using InsertRelation
		err = surrealdb.InsertRelation(s.db.WithContext(ctx), participantRelation)
		if err != nil {
			s.logger.Info(fmt.Sprintf("Error creating relationship between match and participant: %v", err))
			return fmt.Errorf("failed to create match-participant relationship: %w", err)
		}
	}

	s.logger.Info(fmt.Sprintf("Successfully stored match: ID=%d with %d participants",
		match.ID, len(participants)))
	return nil
}

// Close closes the connection to SurrealDB
func (s *SurrealDBClient) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
