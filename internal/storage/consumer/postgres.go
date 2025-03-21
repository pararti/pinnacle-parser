package consumer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL драйвер
	"github.com/pararti/pinnacle-parser/internal/models/parsed"
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
		return err
	}

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

// StoreMatch сохраняет матч в базе данных
func (p *PostgresDBClient) StoreMatch(match *parsed.Match) error {
	if match == nil {
		return errors.New("match is nil")
	}

	if match.League == nil {
		return errors.New("match.League is nil")
	}

	// Сохраняем лигу
	if err := p.StoreLeague(match.League); err != nil {
		return err
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

	// Проверяем, существует ли матч
	var exists bool
	err = tx.QueryRowContext(p.ctx, "SELECT EXISTS(SELECT 1 FROM matches WHERE id = $1)", match.ID).Scan(&exists)
	if err != nil {
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
	}

	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	// Сохраняем участников
	if match.Participants != nil && len(match.Participants) > 0 {
		err = p.StoreParticipants(match.ID, match.Participants)
		if err != nil {
			return err
		}
	}

	return nil
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

// DeleteMatch удаляет матч и все связанные данные
func (p *PostgresDBClient) DeleteMatch(id int) error {
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Получаем ID всех odds связанных с матчем
	oddsQuery := `SELECT id FROM odds WHERE matchup_id = $1`
	rows, err := tx.QueryContext(p.ctx, oddsQuery, id)
	if err != nil {
		return err
	}

	var oddIDs []interface{}
	var i int
	for rows.Next() {
		var oddID int
		if err := rows.Scan(&oddID); err != nil {
			rows.Close()
			return err
		}
		oddIDs = append(oddIDs, oddID)
		i++
	}
	rows.Close()

	if err = rows.Err(); err != nil {
		return err
	}

	// Удаляем price_values для всех odds
	if len(oddIDs) > 0 {
		placeholders := make([]string, len(oddIDs))
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		priceValueQuery := fmt.Sprintf("DELETE FROM price_values WHERE odd_id IN (%s)", strings.Join(placeholders, ","))
		_, err = tx.ExecContext(p.ctx, priceValueQuery, oddIDs...)
		if err != nil {
			return err
		}

		// Удаляем odds
		oddQuery := fmt.Sprintf("DELETE FROM odds WHERE id IN (%s)", strings.Join(placeholders, ","))
		_, err = tx.ExecContext(p.ctx, oddQuery, oddIDs...)
		if err != nil {
			return err
		}
	}

	// Удаляем связи участников
	_, err = tx.ExecContext(p.ctx, "DELETE FROM match_participants WHERE match_id = $1", id)
	if err != nil {
		return err
	}

	// Удаляем сам матч
	_, err = tx.ExecContext(p.ctx, "DELETE FROM matches WHERE id = $1", id)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Close закрывает соединение с базой данных
func (p *PostgresDBClient) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
