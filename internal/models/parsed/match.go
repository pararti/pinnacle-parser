package parsed

import (
	"time"
)

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

// CreatePatch creates an RFC7396-compliant patch from this Sport
func (s *Sport) CreatePatch() *Sport {
	if s == nil {
		return nil
	}

	// Create a minimal patch that includes ID for identification
	patch := &Sport{
		ID: s.ID,
	}

	// Add changed fields according to RFC7396 principles
	if s.Changes != nil {
		for field := range s.Changes {
			switch field {
			case "name":
				patch.Name = s.Name
			}
		}
	}

	return patch
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

// CreatePatch creates an RFC7396-compliant patch from this League
func (l *League) CreatePatch() *League {
	if l == nil {
		return nil
	}

	// Create a minimal patch that includes ID for identification
	patch := &League{
		ID: l.ID,
	}

	// Always include Sport to maintain hierarchy (RFC7396 requirement)
	if l.Sport != nil {
		patch.Sport = l.Sport.CreatePatch()
	}

	// Add changed fields according to RFC7396 principles
	if l.Changes != nil {
		for field := range l.Changes {
			switch field {
			case "group":
				patch.Group = l.Group
			case "isHidden":
				patch.IsHidden = l.IsHidden
			case "isPromoted":
				patch.IsPromoted = l.IsPromoted
			case "isSticky":
				patch.IsSticky = l.IsSticky
			case "name":
				patch.Name = l.Name
			case "sequence":
				patch.Sequence = l.Sequence
			}
		}
	}

	return patch
}

type Participant struct {
	Id        int             `json:"id,omitempty"`
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

// CreatePatch creates an RFC7396-compliant patch from this Participant
func (p *Participant) CreatePatch() *Participant {
	if p == nil {
		return nil
	}

	// Create a minimal patch that includes Id for identification
	patch := &Participant{
		Id: p.Id,
	}

	// Add changed fields according to RFC7396 principles
	if p.Changes != nil {
		for field := range p.Changes {
			switch field {
			case "alignment":
				patch.Alignment = p.Alignment
			case "name":
				patch.Name = p.Name
			}
		}
	}

	return patch
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

	// For hierarchical fields, ensure parent objects are included
	if field == "league.name" || field == "league.isPromoted" ||
		field == "league.sport.id" || field == "league.sport.name" {
		// Mark that we need to include the full hierarchy
		m.Changes["_includeHierarchy"] = true
	}
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

// CreatePatch creates an RFC7396-compliant patch from this Match
func (m *Match) CreatePatch() *Match {
	// Create a minimal patch with just the ID for identification
	patch := &Match{
		ID: m.ID,
	}

	// Add changed fields according to RFC7396 principles
	if m.Changes != nil {
		for field := range m.Changes {
			switch field {
			case "bestOfX":
				patch.BestOfX = m.BestOfX
			case "isLive":
				patch.IsLive = m.IsLive
			case "startTime":
				patch.StartTime = m.StartTime
			case "league":
				// When league changes, include full hierarchy with Sport
				if m.League != nil {
					patch.League = m.League.CreatePatch()
				}
			case "participants":
				// Include changed participants
				if len(m.Participants) > 0 {
					patch.Participants = make([]*Participant, 0, len(m.Participants))
					for _, p := range m.Participants {
						if p != nil && p.Changes != nil && len(p.Changes) > 0 {
							patch.Participants = append(patch.Participants, p.CreatePatch())
						}
					}
				}
			}
		}
	}

	return patch
}

func (m *Match) GetUpdate() *Match {
	// Replace with CreatePatch to use RFC7396-compliant patches
	return m.CreatePatch()
}
