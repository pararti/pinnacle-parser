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
