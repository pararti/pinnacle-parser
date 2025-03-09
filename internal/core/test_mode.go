package core

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/pararti/pinnacle-parser/internal/options"

	"github.com/pararti/pinnacle-parser/internal/abstruct"
	"github.com/pararti/pinnacle-parser/internal/models/kafkadata"
	"github.com/pararti/pinnacle-parser/internal/models/parsed"
	"github.com/pararti/pinnacle-parser/internal/models/test"
	"github.com/pararti/pinnacle-parser/pkg/constants"
	"github.com/pararti/pinnacle-parser/pkg/logger"
)

type TestMode struct {
	logger     *logger.Logger
	sender     abstruct.Sender
	matches    map[int]*parsed.Match
	bets       map[int]map[string]*parsed.Straight // Add bets map
	stopChan   chan struct{}
	isRunning  bool
	matchCount int
}

func NewTestMode(l *logger.Logger, s abstruct.Sender) *TestMode {
	return &TestMode{
		logger:     l,
		sender:     s,
		matches:    make(map[int]*parsed.Match),
		bets:       make(map[int]map[string]*parsed.Straight), // Initialize bets map
		stopChan:   make(chan struct{}),
		matchCount: 0,
	}
}

func (t *TestMode) Start(opts *options.Options) {
	if t.isRunning {
		t.logger.Warn("Test mode is already running")
		return
	}

	t.isRunning = true
	t.logger.Info("Starting test mode - generating random matches")
	t.logger.Info(fmt.Sprintf("Sending events to Kafka topic: %s", opts.KafkaTopic))

	// Start the main test loop
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-t.stopChan:
				t.logger.Info("Test mode stopped")
				return
			case <-ticker.C:
				t.generateAndSendEvents(opts.KafkaTopic)
			}
		}
	}()
}

func (t *TestMode) Stop() {
	if !t.isRunning {
		return
	}
	t.stopChan <- struct{}{}
	t.isRunning = false
}

func (t *TestMode) generateAndSendEvents(topic string) {
	// Create new matches (1-3 per tick)
	newMatchCount := rand.Intn(3) + 1
	newMatches := make([]*parsed.Match, 0, newMatchCount)

	for i := 0; i < newMatchCount; i++ {
		match := test.GenerateExampleMatch()
		if match == nil {
			t.logger.Error("Failed to generate match")
			continue
		}
		t.matches[match.ID] = match
		t.bets[match.ID] = make(map[string]*parsed.Straight) // Initialize bets map for new match
		newMatches = append(newMatches, match)
		t.matchCount++

		// Проверяем тип участников и собираем их ID если необходимо
		isParticipantWithId := false
		var participantIds []int

		if match.Participants != nil && len(match.Participants) > 0 {
			// Проверяем, есть ли у участников ID
			for _, p := range match.Participants {
				if p != nil && p.Id > 0 {
					isParticipantWithId = true
					participantIds = append(participantIds, p.Id)
				}
			}
		}

		// Генерируем несколько ставок для нового матча
		straights := test.GenerateExampleStraights(match.ID, isParticipantWithId, participantIds)
		newBets := make([]*parsed.Straight, 0, len(straights))
		for _, straight := range straights {
			if straight == nil || straight.Key == "" {
				continue
			}
			t.bets[match.ID][straight.Key] = straight
			newBets = append(newBets, straight)
		}

		// Отправляем новые ставки
		if len(newBets) > 0 {
			data := kafkadata.Bet{
				EventType: constants.BET_NEW,
				Source:    constants.SOURCE,
				Data:      newBets,
			}
			if jsonData, err := json.Marshal(data); err == nil {
				t.sender.Send(jsonData, &topic)
				t.logger.Info(fmt.Sprintf("New bets for match %d: %d bets created", match.ID, len(newBets)))
			}
		}
	}

	// Отправляем новые матчи
	if len(newMatches) > 0 {
		data := kafkadata.Match{
			EventType: constants.MATCH_NEW,
			Source:    constants.SOURCE,
			Data:      newMatches,
		}
		if jsonData, err := json.Marshal(data); err == nil {
			t.sender.Send(jsonData, &topic)
			// Логируем детали новых матчей
			for _, match := range newMatches {
				t.logger.Info(fmt.Sprintf("New match: ID=%d, Sport=%s, Teams=%s vs %s, StartTime=%s",
					match.ID,
					match.League.Sport.Name,
					match.Participants[0].Name,
					match.Participants[1].Name,
					match.StartTime.Format("2006-01-02 15:04:05")))
			}
			t.logger.Info(fmt.Sprintf("Total matches in system: %d", len(t.matches)))
		}
	}

	// Генерируем обновления для существующих матчей
	updates := make([]*parsed.Match, 0)
	allBetUpdates := make([]*parsed.Straight, 0)
	deletions := make([]int, 0)

	// Выбираем случайные матчи для обновления
	matchKeys := make([]int, 0, len(t.matches))
	for id := range t.matches {
		matchKeys = append(matchKeys, id)
	}

	// Перемешиваем ключи
	rand.Shuffle(len(matchKeys), func(i, j int) {
		matchKeys[i], matchKeys[j] = matchKeys[j], matchKeys[i]
	})

	// Обновляем 1-3 случайных матча
	updateCount := rand.Intn(3) + 1
	for i := 0; i < updateCount && i < len(matchKeys); i++ {
		id := matchKeys[i]
		match := t.matches[id]
		if match == nil {
			continue
		}

		// Обновляем матч
		delta := test.GenerateRandomMatchDelta(match)
		if delta != nil {
			t.matches[id] = delta
			updates = append(updates, delta)

			// Проверяем тип участников и собираем их ID если необходимо
			isParticipantWithId := false
			var participantIds []int

			if match.Participants != nil && len(match.Participants) > 0 {
				// Проверяем, есть ли у участников ID
				for _, p := range match.Participants {
					if p != nil && p.Id > 0 {
						isParticipantWithId = true
						participantIds = append(participantIds, p.Id)
					}
				}
			}

			// Обновляем ставки для матча
			if t.bets[id] != nil && len(t.bets[id]) > 0 {
				straightDeltas := test.GenerateRandomStraightDeltas(t.bets[id], isParticipantWithId, participantIds)
				for key, straightDelta := range straightDeltas {
					if straightDelta != nil {
						t.bets[id][key] = straightDelta
						allBetUpdates = append(allBetUpdates, straightDelta)
					}
				}
			}
		}

		// Небольшой шанс удаления матча
		if rand.Float32() < 0.05 { // 5% шанс удаления
			deletions = append(deletions, id)
			delete(t.matches, id)
			delete(t.bets, id)
		}
	}

	// Отправляем обновления ставок
	if len(allBetUpdates) > 0 {
		data := kafkadata.BetUpd{
			EventType: constants.BET_UPDATE,
			Source:    constants.SOURCE,
			Data:      allBetUpdates,
		}
		if jsonData, err := json.Marshal(data); err == nil {
			t.sender.Send(jsonData, &topic)
			t.logger.Info(fmt.Sprintf("Updated %d bets", len(allBetUpdates)))
		}
	}

	// Отправляем обновления матчей
	if len(updates) > 0 {
		data := kafkadata.MatchUpd{
			EventType: constants.MATCH_UPDATE,
			Source:    constants.SOURCE,
			Data:      updates,
		}
		if jsonData, err := json.Marshal(data); err == nil {
			t.sender.Send(jsonData, &topic)
			// Логируем детали обновлений
			for _, update := range updates {
				changes := make([]string, 0)
				for field := range update.Changes {
					changes = append(changes, field)
				}
				t.logger.Info(fmt.Sprintf("Updated match: ID=%d, Changes: %v", update.ID, changes))
			}
		}
	}

	// Отправляем удаления
	if len(deletions) > 0 {
		data := kafkadata.DeletedMatch{
			EventType: constants.MATCH_DELETE,
			Source:    constants.SOURCE,
			Data:      deletions,
		}
		if jsonData, err := json.Marshal(data); err == nil {
			t.sender.Send(jsonData, &topic)
			t.logger.Info(fmt.Sprintf("Deleted matches: %v", deletions))
			t.logger.Info(fmt.Sprintf("Remaining matches in system: %d", len(t.matches)))
		}
	}
}
