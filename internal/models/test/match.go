package test

import (
	"math/rand"
	"time"

	"github.com/pararti/pinnacle-parser/internal/models/parsed"
)

// Example sport names and their IDs
var exampleSports = []struct {
	id   int
	name string
}{
	//{1, "Soccer"},
	//{2, "Basketball"},
	//{3, "Tennis"},
	//{4, "Hockey"},
	//{5, "Baseball"},
	//{6, "Volleyball"},
	{7, "eSports"},
	//{8, "Cricket"},
	//{9, "Golf"},
	//{10, "American Football"},
	//{11, "Rugby"},
	//{12, "Handball"},
	//{13, "Water Polo"},
}

// Example team names for generating random participants
var exampleTeams = []string{
	"Red Dragons", "Blue Eagles", "Green Lions", "Black Knights",
	"White Tigers", "Golden Hawks", "Silver Wolves", "Purple Phoenix",
	"Royal Guards", "Elite Warriors", "Storm Riders", "Thunder Kings",
}

// GenerateExampleMatch creates a new Match instance with random example data
func GenerateExampleMatch() *parsed.Match {
	// Generate random sport
	sportIdx := rand.Intn(len(exampleSports))
	sport := &parsed.Sport{
		ID:   exampleSports[sportIdx].id,
		Name: exampleSports[sportIdx].name,
	}

	// Generate random league
	league := &parsed.League{
		ID:         rand.Intn(1000) + 1,
		Name:       sport.Name + " League " + string(rune('A'+rand.Intn(3))),
		Group:      "Group " + string(rune('A'+rand.Intn(4))),
		IsHidden:   rand.Float32() < 0.1,  // 10% chance of being hidden
		IsPromoted: rand.Float32() < 0.2,  // 20% chance of being promoted
		IsSticky:   rand.Float32() < 0.15, // 15% chance of being sticky
		Sequence:   rand.Intn(100),
		Sport:      sport,
	}

	// Определяем, будет ли это обычный матч или special матч с нейтральными участниками
	isSpecialMatch := rand.Float32() < 0.3 // 30% шанс special матча с ID участников

	var participants []*parsed.Participant

	if isSpecialMatch {
		// Генерируем 4 special participants с ID и alignment "neutral"
		// Например, для correct score или других специальных ставок
		participants = make([]*parsed.Participant, 4)
		baseId := 1000000 + rand.Intn(1000000) // Base ID

		teamNames := []string{
			"Team A 2:0 Team B",
			"Team A 2:1 Team B",
			"Team B 2:0 Team A",
			"Team B 2:1 Team A",
		}

		for i := 0; i < 4; i++ {
			participants[i] = &parsed.Participant{
				Id:        baseId + i,
				Name:      teamNames[i],
				Alignment: "neutral",
			}
		}
	} else {
		// Generate 2 regular participants (home/away)
		usedIndices := make(map[int]bool)
		participants = make([]*parsed.Participant, 2)
		for i := 0; i < 2; i++ {
			var teamIdx int
			for {
				teamIdx = rand.Intn(len(exampleTeams))
				if !usedIndices[teamIdx] {
					usedIndices[teamIdx] = true
					break
				}
			}
			participants[i] = &parsed.Participant{
				Name:      exampleTeams[teamIdx],
				Alignment: []string{"home", "away"}[i],
			}
		}
	}

	// Generate match
	match := &parsed.Match{
		ID:           rand.Intn(100000) + 1,
		BestOfX:      []int{1, 2, 3, 5}[rand.Intn(4)],
		IsLive:       rand.Float32() < 0.3, // 30% chance of being live
		League:       league,
		Participants: participants,
		StartTime:    time.Now().Add(time.Duration(rand.Intn(168)) * time.Hour), // 0-7 days in the future
		StatusFlag:   parsed.STATUS_CREATED,
	}

	return match
}

// GenerateRandomMatchDelta creates random changes for an existing match
func GenerateRandomMatchDelta(match *parsed.Match) *parsed.Match {
	// Проверяем что match не nil
	if match == nil {
		return nil
	}

	// Create a copy with only the ID for identification
	delta := &parsed.Match{
		ID:           match.ID,
		StatusFlag:   parsed.STATUS_UPDATED,
		Changes:      make(map[string]bool),
		Participants: make([]*parsed.Participant, len(match.Participants)),
	}

	// 20% chance to change startTime
	if rand.Float32() < 0.2 {
		// Random adjustment between -1 and +2 hours
		hoursAdjustment := time.Duration(rand.Intn(3)-1) * time.Hour
		delta.StartTime = match.StartTime.Add(hoursAdjustment)
		delta.MarkChanged("startTime")
	}

	// 15% chance to change bestOfX
	if rand.Float32() < 0.15 {
		newBestOfX := rand.Intn(5)*2 + 1 // 1, 3, 5, 7, 9
		if newBestOfX != match.BestOfX {
			delta.BestOfX = newBestOfX
			delta.MarkChanged("bestOfX")
		}
	}

	// 10% chance to change isLive status
	if rand.Float32() < 0.1 {
		delta.IsLive = !match.IsLive
		delta.MarkChanged("isLive")
	}

	// League changes (25% chance for any league change)
	if rand.Float32() < 0.25 && match.League != nil && match.League.Sport != nil {
		// Always include Sport to maintain RFC7396 object hierarchy
		sportObj := &parsed.Sport{
			ID:   match.League.Sport.ID,
			Name: match.League.Sport.Name,
		}

		delta.League = &parsed.League{
			ID:    match.League.ID,
			Sport: sportObj,
		}
		delta.MarkChanged("league")

		// Name change (15% chance)
		if rand.Float32() < 0.15 {
			delta.League.Name = match.League.Sport.Name + " League " + string(rune('A'+rand.Intn(3)))
			delta.League.MarkChanged("name")
		}

		// IsPromoted flag change (20% chance)
		if rand.Float32() < 0.2 {
			delta.League.IsPromoted = !match.League.IsPromoted
			delta.League.MarkChanged("isPromoted")
		}
	}

	// Participant changes (35% chance for any participant change)
	if rand.Float32() < 0.35 && match.Participants != nil && len(match.Participants) > 0 {
		delta.MarkChanged("participants")
		for i := range match.Participants {
			// Проверяем что индекс в пределах массива
			if i >= len(delta.Participants) {
				continue
			}

			// Проверяем что участник не nil
			if match.Participants[i] == nil {
				delta.Participants[i] = &parsed.Participant{}
				continue
			}

			// Create RFC7396-compliant participant patch with ID
			delta.Participants[i] = &parsed.Participant{
				Id: match.Participants[i].Id,
			}

			// 20% chance to change team name
			if rand.Float32() < 0.2 {
				newTeam := exampleTeams[rand.Intn(len(exampleTeams))]
				if newTeam != match.Participants[i].Name {
					delta.Participants[i].Name = newTeam
					delta.Participants[i].MarkChanged("name")
				}
			}
		}
	}

	return delta
}
