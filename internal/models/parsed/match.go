package parsed

import (
	"math/rand"
	"time"
)

// Example sport names and their IDs
var exampleSports = []struct {
	id   int
	name string
}{
	{1, "Soccer"},
	{2, "Basketball"},
	{3, "Tennis"},
	{4, "Hockey"},
	{5, "Baseball"},
	{6, "Volleyball"},
	{7, "eSports"},
}

// Example team names for generating random participants
var exampleTeams = []string{
	"Red Dragons", "Blue Eagles", "Green Lions", "Black Knights",
	"White Tigers", "Golden Hawks", "Silver Wolves", "Purple Phoenix",
	"Royal Guards", "Elite Warriors", "Storm Riders", "Thunder Kings",
}

type Sport struct {
	ID      int             `json:"id,omitempty"`
	Name    string          `json:"name,omitempty"`
	Changes map[string]bool `json:"-"`
}

func (s *Sport) MarkChanged(field string) {
	if s.Changes == nil {
		s.Changes = make(map[string]bool, 2)
	}
	s.Changes[field] = true
}

func (s *Sport) getUpdate() *Sport {
	if len(s.Changes) == 0 {
		return nil
	}
	upd := &Sport{}
	for field := range s.Changes {
		if field == "name" {
			upd.Name = s.Name
			continue
		}
		if field == "id" {
			upd.ID = s.ID
		}
	}

	return upd
}

type League struct {
	Group      string          `json:"group,omitempty"`
	ID         int             `json:"id,omitempty"`
	IsHidden   bool            `json:"isHidden,omitempty"`
	IsPromoted bool            `json:"isPromoted,omitempty"`
	IsSticky   bool            `json:"isSticky,omitempty"`
	Name       string          `json:"name,omitempty"`
	Sequence   int             `json:"sequence,omitempty"`
	Sport      *Sport          `json:"sport,omitempty"`
	Changes    map[string]bool `json:"-"`
}

func (l *League) MarkChanged(field string) {
	if l.Changes == nil {
		l.Changes = make(map[string]bool, 8)
	}
	l.Changes[field] = true
}

func (l *League) getUpdate() *League {
	if len(l.Changes) == 0 {
		return nil
	}
	upd := &League{}
	for field := range l.Changes {
		if field == "group" {
			upd.Group = l.Group
			continue
		}
		if field == "id" {
			upd.ID = l.ID
			continue
		}
		if field == "isHidden" {
			upd.IsHidden = l.IsHidden
			continue
		}
		if field == "isPromoted" {
			upd.IsPromoted = l.IsPromoted
			continue
		}
		if field == "isSticky" {
			upd.IsSticky = l.IsSticky
			continue
		}
		if field == "name" {
			upd.Name = l.Name
			continue
		}
		if field == "sequence" {
			upd.Sequence = l.Sequence
			continue
		}
		if field == "sport" {
			upd.Sport = l.Sport.getUpdate()
		}
	}

	return upd
}

type Participant struct {
	Alignment string          `json:"alignment,omitempty"`
	Name      string          `json:"name,omitempty"`
	Changes   map[string]bool `json:"-"`
}

func (p *Participant) MarkChanged(field string) {
	if p.Changes == nil {
		p.Changes = make(map[string]bool, 2)
	}
	p.Changes[field] = true
}

type Match struct {
	BestOfX      int             `json:"bestOfX,omitempty"`
	ID           int             `json:"id,omitempty"`
	IsLive       bool            `json:"isLive,omitempty"`
	League       *League         `json:"league,omitempty"`
	Participants []*Participant  `json:"participants,omitempty"`
	StartTime    time.Time       `json:"startTime,omitempty"`
	ParentId     int             `json:"parentId,omitempty"`
	StatusFlag   int8            `json:"-"`
	Changes      map[string]bool `json:"-"`
}

func (m *Match) MarkChanged(field string) {
	if m.Changes == nil {
		m.Changes = make(map[string]bool, 6)
	}
	m.Changes[field] = true
}

func (m *Match) getParcipantUpdate() []*Participant {
	updParcs := make([]*Participant, 0, len(m.Participants))
	for _, p := range m.Participants {
		if p.Changes == nil {
			continue
		}
		parc := &Participant{}
		for field := range p.Changes {
			if field == "alignment" {
				parc.Alignment = p.Alignment
				continue
			}
			if field == "name" {
				parc.Name = p.Name
			}
		}
		updParcs = append(updParcs, parc)
	}

	return updParcs
}

func (m *Match) GetUpdate() *Match {
	upd := &Match{}
	upd.ID = m.ID
	for field := range m.Changes {
		if field == "bestOfX" {
			upd.BestOfX = m.BestOfX
			continue
		}
		if field == "isLive" {
			upd.IsLive = m.IsLive
			continue
		}
		if field == "league" {
			upd.League = m.League.getUpdate()
			continue
		}
		if field == "participants" {
			upd.Participants = m.getParcipantUpdate()
			continue
		}
		if field == "startTime" {
			upd.StartTime = m.StartTime
		}
	}

	return upd
}

// GenerateExampleMatch creates a new Match instance with random example data
func GenerateExampleMatch() *Match {
	// Generate random sport
	sportIdx := rand.Intn(len(exampleSports))
	sport := &Sport{
		ID:   exampleSports[sportIdx].id,
		Name: exampleSports[sportIdx].name,
	}

	// Generate random league
	league := &League{
		ID:         rand.Intn(1000) + 1,
		Name:       sport.Name + " League " + string(rune('A'+rand.Intn(3))),
		Group:      "Group " + string(rune('A'+rand.Intn(4))),
		IsHidden:   rand.Float32() < 0.1,  // 10% chance of being hidden
		IsPromoted: rand.Float32() < 0.2,  // 20% chance of being promoted
		IsSticky:   rand.Float32() < 0.15, // 15% chance of being sticky
		Sequence:   rand.Intn(100),
		Sport:      sport,
	}

	// Generate 2 random participants
	usedIndices := make(map[int]bool)
	participants := make([]*Participant, 2)
	for i := 0; i < 2; i++ {
		var teamIdx int
		for {
			teamIdx = rand.Intn(len(exampleTeams))
			if !usedIndices[teamIdx] {
				usedIndices[teamIdx] = true
				break
			}
		}
		participants[i] = &Participant{
			Name:      exampleTeams[teamIdx],
			Alignment: []string{"home", "away"}[i],
		}
	}

	// Generate match
	match := &Match{
		ID:           rand.Intn(100000) + 1,
		BestOfX:      []int{1, 2, 3, 5}[rand.Intn(4)],
		IsLive:       rand.Float32() < 0.3, // 30% chance of being live
		League:       league,
		Participants: participants,
		StartTime:    time.Now().Add(time.Duration(rand.Intn(168)) * time.Hour), // Random time within next week
		ParentId:     0,                                                         // Will be set to ID if not specified
		StatusFlag:   STATUS_CREATED,
	}
	match.ParentId = match.ID

	return match
}
