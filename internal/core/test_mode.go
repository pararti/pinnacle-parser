package core

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/pararti/pinnacle-parser/internal/abstruct"
	"github.com/pararti/pinnacle-parser/internal/models/kafkadata"
	"github.com/pararti/pinnacle-parser/internal/models/parsed"
	"github.com/pararti/pinnacle-parser/pkg/constants"
	"github.com/pararti/pinnacle-parser/pkg/logger"
)

type TestMode struct {
	logger     *logger.Logger
	sender     abstruct.Sender
	matches    map[int]*parsed.Match
	stopChan   chan struct{}
	isRunning  bool
	matchCount int
}

func NewTestMode(l *logger.Logger, s abstruct.Sender) *TestMode {
	return &TestMode{
		logger:     l,
		sender:     s,
		matches:    make(map[int]*parsed.Match),
		stopChan:   make(chan struct{}),
		matchCount: 0,
	}
}

func (t *TestMode) Start(topic string) {
	if t.isRunning {
		t.logger.Warn("Test mode is already running")
		return
	}

	t.isRunning = true
	t.logger.Info("Starting test mode - generating random matches")
	t.logger.Info(fmt.Sprintf("Sending events to Kafka topic: %s", topic))

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
				t.generateAndSendEvents(topic)
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
		match := parsed.GenerateExampleMatch()
		t.matches[match.ID] = match
		newMatches = append(newMatches, match)
		t.matchCount++
	}

	// Send new matches
	if len(newMatches) > 0 {
		data := kafkadata.Match{
			EventType: constants.MATCH_NEW,
			Source:    constants.SOURCE,
			Data:      newMatches,
		}
		if jsonData, err := json.Marshal(data); err == nil {
			t.sender.Send(jsonData, &topic)
			// Log details of each new match
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

	// Generate updates for existing matches (30% chance per match)
	updates := make([]*parsed.Match, 0)
	deletions := make([]int, 0)

	for id, match := range t.matches {
		if rand.Float32() < 0.3 { // 30% chance to update
			delta := parsed.GenerateRandomMatchDelta(match)
			updates = append(updates, delta)
		}

		if rand.Float32() < 0.05 { // 5% chance to delete
			deletions = append(deletions, id)
			delete(t.matches, id)
		}
	}

	// Send updates
	if len(updates) > 0 {
		data := kafkadata.MatchUpd{
			EventType: constants.MATCH_UPDATE,
			Source:    constants.SOURCE,
			Data:      updates,
		}
		if jsonData, err := json.Marshal(data); err == nil {
			t.sender.Send(jsonData, &topic)
			// Log details of each update
			for _, update := range updates {
				changes := make([]string, 0)
				for field := range update.Changes {
					changes = append(changes, field)
				}
				t.logger.Info(fmt.Sprintf("Updated match: ID=%d, Changes: %v", update.ID, changes))
			}
		}
	}

	// Send deletions
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
