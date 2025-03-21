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

	// Проверяем, существует ли ставка
	var straightID int
	err = tx.QueryRowContext(
		p.ctx,
		"SELECT id FROM straights WHERE key = $1 AND matchup_id = $2",
		straight.Key,
		straight.MatchupID,
	).Scan(&straightID)

	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if err == sql.ErrNoRows {
		// Создаем новую ставку
		query := `
			INSERT INTO straights (key, matchup_id, period, side, status, type)
			VALUES ($1, $2, $3, $4, $5, $6)
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
		).Scan(&straightID)
	} else {
		// Обновляем существующую ставку
		query := `
			UPDATE straights SET 
				period = $1, 
				side = $2, 
				status = $3, 
				type = $4
			WHERE id = $5
		`
		_, err = tx.ExecContext(
			p.ctx,
			query,
			straight.Period,
			straight.Side,
			straight.Status,
			straight.Type,
			straightID,
		)
	}

	if err != nil {
		return err
	}

	// Обновляем цены
	if straight.Prices != nil && len(straight.Prices) > 0 {
		// Удаляем существующие цены
		_, err = tx.ExecContext(p.ctx, "DELETE FROM prices WHERE straight_id = $1", straightID)
		if err != nil {
			return err
		}

		// Добавляем новые цены
		for _, price := range straight.Prices {
			if price == nil {
				continue
			}

			query := `
				INSERT INTO prices (straight_id, designation, price, points, participant_id)
				VALUES ($1, $2, $3, $4, $5)
			`
			_, err = tx.ExecContext(
				p.ctx,
				query,
				straightID,
				price.Designation,
				price.Price,
				price.Points,
				price.ParticipantId,
			)
			if err != nil {
				return err
			}
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

	// Удаляем связанные цены и ставки
	straightsQuery := `SELECT id FROM straights WHERE matchup_id = $1`
	rows, err := tx.QueryContext(p.ctx, straightsQuery, id)
	if err != nil {
		return err
	}

	var straightIDs []interface{}
	var i int
	for rows.Next() {
		var straightID int
		if err := rows.Scan(&straightID); err != nil {
			rows.Close()
			return err
		}
		straightIDs = append(straightIDs, straightID)
		i++
	}
	rows.Close()

	if err = rows.Err(); err != nil {
		return err
	}

	// Удаляем цены для всех ставок
	if len(straightIDs) > 0 {
		placeholders := make([]string, len(straightIDs))
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		priceQuery := fmt.Sprintf("DELETE FROM prices WHERE straight_id IN (%s)", strings.Join(placeholders, ","))
		_, err = tx.ExecContext(p.ctx, priceQuery, straightIDs...)
		if err != nil {
			return err
		}

		// Удаляем ставки
		straightQuery := fmt.Sprintf("DELETE FROM straights WHERE id IN (%s)", strings.Join(placeholders, ","))
		_, err = tx.ExecContext(p.ctx, straightQuery, straightIDs...)
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
