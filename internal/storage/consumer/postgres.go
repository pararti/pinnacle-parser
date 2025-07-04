package consumer

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL драйвер
	"github.com/pararti/pinnacle-parser/internal/models/parsed"
	"github.com/pararti/pinnacle-parser/pkg/jsonpatch"
	"github.com/pararti/pinnacle-parser/pkg/logger"
)

// PostgresDBClient управляет соединением с PostgreSQL (Supabase)
type PostgresDBClient struct {
	db     *sql.DB
	logger *logger.Logger
	ctx    context.Context
}

// NewPostgresDBClient создает новое подключение к PostgreSQL (Supabase)
func NewPostgresDBClient(connectionString string, logger *logger.Logger) (*PostgresDBClient, error) {
	ctx := context.Background()

	// Открываем соединение с базой данных
	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		return nil, err
	}

	// Проверяем соединение
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	// Устанавливаем параметры пула соединений
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	client := &PostgresDBClient{
		db:     db,
		logger: logger,
		ctx:    ctx,
	}

	logger.Info("Successfully connected to PostgreSQL database")
	return client, nil
}

// SportRecord представляет запись Sport в базе данных
type SportRecord struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// LeagueRecord представляет запись League в базе данных
type LeagueRecord struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	Group      string `json:"group,omitempty"`
	IsHidden   bool   `json:"isHidden"`
	IsPromoted bool   `json:"isPromoted"`
	IsSticky   bool   `json:"isSticky"`
	Sequence   int    `json:"sequence"`
	SportID    int    `json:"sport_id"`
}

// TeamRecord представляет запись команды в базе данных
type TeamRecord struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// MatchParticipantRecord представляет запись участника матча в базе данных
type MatchParticipantRecord struct {
	ID        int    `json:"id"`
	MatchID   int    `json:"match_id"`
	TeamID    int    `json:"team_id"`
	Alignment string `json:"alignment"`
}

// MatchRecord представляет запись матча в базе данных
type MatchRecord struct {
	ID        int       `json:"id"`
	BestOfX   int       `json:"best_of_x"`
	IsLive    bool      `json:"is_live"`
	StartTime time.Time `json:"start_time"`
	ParentID  int       `json:"parent_id"`
	LeagueID  int       `json:"league_id"`
	Status    string    `json:"status"`
}

// StraightRecord представляет запись ставки в базе данных
type StraightRecord struct {
	ID        int    `json:"id"`
	Key       string `json:"key"`
	MatchupID int    `json:"matchup_id"`
	Period    int    `json:"period"`
	Side      string `json:"side"`
	Status    string `json:"status"`
	Type      string `json:"type"`
}

// PriceRecord представляет запись цены в базе данных
type PriceRecord struct {
	ID            int     `json:"id"`
	StraightID    int     `json:"straight_id"`
	Designation   string  `json:"designation"`
	Price         int     `json:"price"`
	Points        float64 `json:"points"`
	ParticipantID int     `json:"participant_id"`
}

// OddRecord представляет запись коэффициента в базе данных
type OddRecord struct {
	ID            int     `json:"id"`
	Key           string  `json:"key"`
	MatchupID     int     `json:"matchup_id"`
	Period        int     `json:"period"`
	Side          string  `json:"side"`
	Status        string  `json:"status"`
	Type          string  `json:"type"`
	Designation   string  `json:"designation"`
	Points        float64 `json:"points"`
	ParticipantID int     `json:"participant_id"`
	LatestPrice   int     `json:"latest_price"`
}

// PriceValueRecord представляет запись значения цены в базе данных
type PriceValueRecord struct {
	ID        int       `json:"id"`
	OddID     int       `json:"odd_id"`
	Value     int       `json:"value"`
	CreatedAt time.Time `json:"created_at"`
}

// StoreSport сохраняет вид спорта в базе данных
func (p *PostgresDBClient) StoreSport(sport *parsed.Sport) error {
	if sport == nil {
		return errors.New("sport is nil")
	}

	query := `
		INSERT INTO sports (id, name)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE
		SET name = $2
	`

	_, err := p.db.ExecContext(p.ctx, query, sport.ID, sport.Name)
	if err != nil {
		return err
	}

	return nil
}

// StoreLeague сохраняет лигу в базе данных
func (p *PostgresDBClient) StoreLeague(league *parsed.League) error {
	if league == nil {
		return errors.New("league is nil")
	}

	if league.Sport == nil {
		return errors.New("league.Sport is nil")
	}

	// Убедимся, что Sport существует
	if err := p.StoreSport(league.Sport); err != nil {
		p.logger.Error("Failed to store sport", league.Sport.ID, err)
		return err
	}

	query := `
		INSERT INTO leagues (id, sport_id, name, group_name, is_hidden, is_promoted, is_sticky, sequence)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE
		SET 
			sport_id = $2, 
			name = $3, 
			group_name = $4, 
			is_hidden = $5, 
			is_promoted = $6, 
			is_sticky = $7, 
			sequence = $8
	`

	_, err := p.db.ExecContext(
		p.ctx,
		query,
		league.ID,
		league.Sport.ID,
		league.Name,
		league.Group,
		league.IsHidden,
		league.IsPromoted,
		league.IsSticky,
		league.Sequence,
	)

	if err != nil {
		p.logger.Error("Failed to execute league query", league.ID, err)
		return err
	}

	p.logger.Info("Stored league: id=", league.ID, ", sportId=", league.Sport.ID)
	return nil
}

// FindOrCreateTeam находит или создает запись команды
func (p *PostgresDBClient) FindOrCreateTeam(part *parsed.Participant) (int, error) {
	if part == nil {
		return 0, errors.New("participant is empty")
	}

	// Сначала пытаемся найти команду
	query := `SELECT id FROM teams WHERE name = $1 LIMIT 1`

	var teamId int
	err := p.db.QueryRowContext(p.ctx, query, part.Name).Scan(&teamId)
	if err == nil {
		return teamId, nil
	}

	if err != sql.ErrNoRows {
		return 0, err
	}

	// Если команда не найдена, создаем новую
	insertQuery := `INSERT INTO teams (name) VALUES ($1) RETURNING id`

	err = p.db.QueryRowContext(p.ctx, insertQuery, part.Name).Scan(&teamId)
	if err != nil {
		return 0, err
	}

	return teamId, nil
}

// StoreParticipants сохраняет участников матча в базе данных
func (p *PostgresDBClient) StoreParticipants(matchID int, participants []*parsed.Participant) error {
	if len(participants) == 0 {
		return errors.New("participants list is empty")
	}

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Сначала удаляем существующие связи
	_, err = tx.ExecContext(p.ctx, "DELETE FROM match_participants WHERE match_id = $1", matchID)
	if err != nil {
		return err
	}

	// Создаем участников и связи
	for _, participant := range participants {
		if participant == nil {
			continue
		}

		// Находим или создаем команду
		teamId, err := p.FindOrCreateTeam(participant)
		if err != nil {
			return err
		}

		// Создаем связь матч-участник
		_, err = tx.ExecContext(
			p.ctx,
			"INSERT INTO match_participants (match_id, team_id, alignment) VALUES ($1, $2, $3)",
			matchID,
			teamId,
			participant.Alignment,
		)
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// GetMatchByID retrieves a match by ID including its League and Sport data
func (p *PostgresDBClient) GetMatchByID(matchID int) (*parsed.Match, error) {
	query := `
		SELECT m.id, m.best_of_x, m.is_live, m.start_time, m.parent_id,
			l.id, l.name, l.group_name, l.is_hidden, l.is_promoted, l.is_sticky, l.sequence,
			s.id, s.name
		FROM matches m
		JOIN leagues l ON m.league_id = l.id
		JOIN sports s ON l.sport_id = s.id
		WHERE m.id = $1
	`

	var match parsed.Match
	var league parsed.League
	var sport parsed.Sport

	err := p.db.QueryRowContext(p.ctx, query, matchID).Scan(
		&match.ID, &match.BestOfX, &match.IsLive, &match.StartTime, &match.ParentId,
		&league.ID, &league.Name, &league.Group, &league.IsHidden, &league.IsPromoted, &league.IsSticky, &league.Sequence,
		&sport.ID, &sport.Name,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Match not found, return nil without error
		}
		return nil, err
	}

	// Build the complete structure
	league.Sport = &sport
	match.League = &league

	// Fetch participants
	participantsQuery := `
		SELECT mp.team_id, mp.alignment, t.name
		FROM match_participants mp
		JOIN teams t ON mp.team_id = t.id
		WHERE mp.match_id = $1
	`

	rows, err := p.db.QueryContext(p.ctx, participantsQuery, matchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	participants := make([]*parsed.Participant, 0)
	for rows.Next() {
		var participant parsed.Participant
		var teamID int

		if err := rows.Scan(&teamID, &participant.Alignment, &participant.Name); err != nil {
			return nil, err
		}

		participant.Id = teamID
		participants = append(participants, &participant)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	match.Participants = participants

	return &match, nil
}

// StoreMatch сохраняет матч в базе данных
func (p *PostgresDBClient) StoreMatch(patch *parsed.Match) error {
	if patch == nil {
		return errors.New("match patch is nil")
	}

	// Check if match exists
	var exists bool
	err := p.db.QueryRowContext(p.ctx, "SELECT EXISTS(SELECT 1 FROM matches WHERE id = $1)", patch.ID).Scan(&exists)
	if err != nil {
		return err
	}

	if exists {
		// For existing matches, apply RFC7396 merge patch
		existing, err := p.GetMatchByID(patch.ID)
		if err != nil {
			p.logger.Error("Failed to get existing match for patching", patch.ID, err)
			return err
		}

		if existing == nil {
			p.logger.Warn("Match exists in database but GetMatchByID returned nil", patch.ID)
			// Fallback to treating it as a new match
			return p.storeCompleteMatch(patch)
		}

		// Apply merge patch
		merged, err := jsonpatch.ApplyMergePatch(existing, patch)
		if err != nil {
			p.logger.Error("Failed to apply patch to match", patch.ID, err)
			return err
		}

		// Convert to Match type
		mergedMatch, ok := merged.(*parsed.Match)
		if !ok {
			return errors.New("merge result is not a Match")
		}

		// Continue with storage using the merged object
		return p.storeCompleteMatch(mergedMatch)
	} else {
		// For new matches, require complete data
		if patch.League == nil {
			return errors.New("match.League is nil")
		}

		if patch.League.Sport == nil {
			return errors.New("match.League.Sport is nil")
		}

		// Store as a new match
		return p.storeCompleteMatch(patch)
	}
}

// storeCompleteMatch handles the actual storage of a complete match
func (p *PostgresDBClient) storeCompleteMatch(match *parsed.Match) error {
	if match == nil {
		return errors.New("match is nil")
	}

	if match.League == nil {
		return errors.New("match.League is nil")
	}

	// Check if league.Sport is nil
	if match.League.Sport == nil {
		p.logger.Error("match.League.Sport is nil for match", match.ID)
		return errors.New("match.League.Sport is nil")
	}

	// Log match details
	p.logger.Info("Storing match: ID=", match.ID,
		", League=", match.League.ID,
		", Sport=", match.League.Sport.ID,
		", Teams=", getTeamsString(match.Participants))

	// Сохраняем лигу
	if err := p.StoreLeague(match.League); err != nil {
		p.logger.Error("Failed to store league for match", match.ID, err)
		return err
	}

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		p.logger.Error("Failed to begin transaction for match", match.ID, err)
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Проверяем, существует ли матч
	var exists bool
	err = tx.QueryRowContext(p.ctx, "SELECT EXISTS(SELECT 1 FROM matches WHERE id = $1)", match.ID).Scan(&exists)
	if err != nil {
		p.logger.Error("Failed to check if match exists", match.ID, err)
		return err
	}

	var query string
	if exists {
		// Обновляем существующий матч
		query = `
			UPDATE matches SET 
				best_of_x = $1, 
				is_live = $2, 
				league_id = $3, 
				start_time = $4, 
				parent_id = $5
			WHERE id = $6
		`
		_, err = tx.ExecContext(
			p.ctx,
			query,
			match.BestOfX,
			match.IsLive,
			match.League.ID,
			match.StartTime,
			match.ParentId,
			match.ID,
		)
		if err != nil {
			p.logger.Error("Failed to update match", match.ID, err)
		} else {
			p.logger.Info("Updated match in database: ID=", match.ID)
		}
	} else {
		// Создаем новый матч
		query = `
			INSERT INTO matches (id, best_of_x, is_live, league_id, start_time, parent_id)
			VALUES ($1, $2, $3, $4, $5, $6)
		`
		_, err = tx.ExecContext(
			p.ctx,
			query,
			match.ID,
			match.BestOfX,
			match.IsLive,
			match.League.ID,
			match.StartTime,
			match.ParentId,
		)
		if err != nil {
			p.logger.Error("Failed to insert new match", match.ID, err)
		} else {
			p.logger.Info("Inserted new match in database: ID=", match.ID)
		}
	}

	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		p.logger.Error("Failed to commit transaction for match", match.ID, err)
		return err
	}

	// Сохраняем участников
	if match.Participants != nil && len(match.Participants) > 0 {
		err = p.StoreParticipants(match.ID, match.Participants)
		if err != nil {
			p.logger.Error("Failed to store participants for match", match.ID, err)
			return err
		}
		p.logger.Info("Stored participants for match: ID=", match.ID, ", Count=", len(match.Participants))
	} else {
		p.logger.Warn("No participants to store for match", match.ID)
	}

	return nil
}

// Helper function to get teams string representation
func getTeamsString(participants []*parsed.Participant) string {
	if participants == nil || len(participants) == 0 {
		return "no participants"
	}

	result := ""
	for i, p := range participants {
		if p == nil {
			continue
		}
		if i > 0 {
			result += " vs "
		}
		result += p.Name
	}
	return result
}

// StoreStraight сохраняет ставку в базе данных
func (p *PostgresDBClient) StoreStraight(straight *parsed.Straight) error {
	if straight == nil {
		return errors.New("straight is nil")
	}

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Для каждой цены создаем отдельную запись odd
	for _, price := range straight.Prices {
		if price == nil {
			continue
		}

		// Проверяем, существует ли odd
		var oddID int
		err = tx.QueryRowContext(
			p.ctx,
			`SELECT id FROM odds WHERE key = $1 AND matchup_id = $2 AND 
			period = $3 AND side = $4 AND type = $5 AND designation = $6 AND 
			(participant_id = $7 OR (participant_id IS NULL AND $7 IS NULL))`,
			straight.Key,
			straight.MatchupID,
			straight.Period,
			straight.Side,
			straight.Type,
			price.Designation,
			price.ParticipantId,
		).Scan(&oddID)

		if err != nil && err != sql.ErrNoRows {
			return err
		}

		if err == sql.ErrNoRows {
			// Создаем новый odd
			query := `
				INSERT INTO odds (key, matchup_id, period, side, status, type, designation, points, participant_id, latest_price)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				RETURNING id
			`
			err = tx.QueryRowContext(
				p.ctx,
				query,
				straight.Key,
				straight.MatchupID,
				straight.Period,
				straight.Side,
				straight.Status,
				straight.Type,
				price.Designation,
				price.Points,
				price.ParticipantId,
				price.Price,
			).Scan(&oddID)
		} else {
			// Обновляем существующий odd
			query := `
				UPDATE odds SET 
					period = $1, 
					side = $2, 
					status = $3, 
					type = $4,
					points = $5,
					latest_price = $6
				WHERE id = $7
			`
			_, err = tx.ExecContext(
				p.ctx,
				query,
				straight.Period,
				straight.Side,
				straight.Status,
				straight.Type,
				price.Points,
				price.Price,
				oddID,
			)
		}

		if err != nil {
			return err
		}

		// Добавляем новую запись цены
		query := `
			INSERT INTO price_values (odd_id, value)
			VALUES ($1, $2)
		`
		_, err = tx.ExecContext(
			p.ctx,
			query,
			oddID,
			price.Price,
		)
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// DeleteMatch marks a match as deleted by updating its status
func (p *PostgresDBClient) DeleteMatch(id int) error {
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		p.logger.Error("Failed to begin transaction for deleting match", id, err)
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Update match status to 'deleted'
	query := `
		UPDATE matches 
		SET status = 'deleted', updated_at = CURRENT_TIMESTAMP 
		WHERE id = $1
	`
	_, err = tx.ExecContext(p.ctx, query, id)
	if err != nil {
		p.logger.Error("Failed to mark match as deleted", id, err)
		return err
	}

	// Update related odds status to 'deleted'
	oddsQuery := `
		UPDATE odds 
		SET status = 'deleted', updated_at = CURRENT_TIMESTAMP 
		WHERE matchup_id = $1
	`
	_, err = tx.ExecContext(p.ctx, oddsQuery, id)
	if err != nil {
		p.logger.Error("Failed to mark odds as deleted for match", id, err)
		return err
	}

	if err = tx.Commit(); err != nil {
		p.logger.Error("Failed to commit transaction for deleting match", id, err)
		return err
	}

	p.logger.Info("Successfully deleted match: ID=", id)
	return nil
}

// Close закрывает соединение с базой данных
func (p *PostgresDBClient) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
