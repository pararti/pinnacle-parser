package storage

import (
	"github.com/pararti/pinnacle-parser/internal/models/parsed"
	"sync"
)

type MapStorage struct {
	MatchUpdChan chan int
	MatchNewChan chan int
	BetUpdChan   chan int
	BetNewChan   chan int
	mu           sync.RWMutex
	MatchDelChan chan []int
	Matches      map[int]*parsed.Match
	Bets         map[int]map[string]*parsed.Straight
}

func NewMapStorage() *MapStorage {
	m := make(map[int]*parsed.Match, 64)
	b := make(map[int]map[string]*parsed.Straight, 64)
	muc := make(chan int, 1)
	mnc := make(chan int, 1)
	mdc := make(chan []int, 8)
	buc := make(chan int, 1)
	bnc := make(chan int, 1)

	return &MapStorage{Matches: m, Bets: b, MatchUpdChan: muc, MatchNewChan: mnc, MatchDelChan: mdc, BetUpdChan: buc, BetNewChan: bnc}
}

func (m *MapStorage) SetMatches(matches []*parsed.Match) {
	m.mu.Lock()
	ids := make(map[int]struct{}, len(matches))
	upd := 0
	newy := 0
	if matches[0].ParentId == 0 {
		matches[0].ParentId = matches[0].ID
	}
	parentId := matches[0].ParentId
	for _, match := range matches {
		ids[match.ID] = struct{}{}
		if match.ParentId == 0 {
			match.ParentId = parentId
		}
		_, ok := m.Matches[match.ID]
		if !ok {
			match.StatusFlag = parsed.STATUS_CREATED
			m.Matches[match.ID] = match
			newy++
			continue
		}

		//проверяем изменения и записываем их в мапу
		var status int8 = parsed.STATUS_NOT_CHANGE
		if m.Matches[match.ID].BestOfX != match.BestOfX {
			m.Matches[match.ID].BestOfX = match.BestOfX
			m.Matches[match.ID].MarkChanged("bestOfX")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].IsLive != match.IsLive {
			m.Matches[match.ID].IsLive = match.IsLive
			m.Matches[match.ID].MarkChanged("isLive")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].League.Group != match.League.Group {
			m.Matches[match.ID].League.Group = match.League.Group
			m.Matches[match.ID].MarkChanged("league")
			m.Matches[match.ID].League.MarkChanged("group")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].League.ID != match.League.ID {
			m.Matches[match.ID].League.ID = match.League.ID
			m.Matches[match.ID].MarkChanged("league")
			m.Matches[match.ID].League.MarkChanged("id")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].League.IsHidden != match.League.IsHidden {
			m.Matches[match.ID].League.IsHidden = match.League.IsHidden
			m.Matches[match.ID].MarkChanged("league")
			m.Matches[match.ID].League.MarkChanged("isHidden")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].League.IsPromoted != match.League.IsPromoted {
			m.Matches[match.ID].League.IsPromoted = match.League.IsPromoted
			m.Matches[match.ID].MarkChanged("league")
			m.Matches[match.ID].League.MarkChanged("isPromoted")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].League.IsSticky != match.League.IsSticky {
			m.Matches[match.ID].League.IsSticky = match.League.IsSticky
			m.Matches[match.ID].MarkChanged("league")
			m.Matches[match.ID].League.MarkChanged("isSticky")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].League.Name != match.League.Name {
			m.Matches[match.ID].League.Name = match.League.Name
			m.Matches[match.ID].MarkChanged("league")
			m.Matches[match.ID].League.MarkChanged("name")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].League.Sequence != match.League.Sequence {
			m.Matches[match.ID].League.Sequence = match.League.Sequence
			m.Matches[match.ID].MarkChanged("league")
			m.Matches[match.ID].League.MarkChanged("sequence")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].League.Sport.ID != match.League.Sport.ID {
			m.Matches[match.ID].League.Sport.ID = match.League.Sport.ID
			m.Matches[match.ID].MarkChanged("league")
			m.Matches[match.ID].League.MarkChanged("sport")
			m.Matches[match.ID].League.Sport.MarkChanged("id")
			status = parsed.STATUS_UPDATED
		}
		if m.Matches[match.ID].League.Sport.Name != match.League.Sport.Name {
			m.Matches[match.ID].League.Sport.Name = match.League.Sport.Name
			m.Matches[match.ID].MarkChanged("league")
			m.Matches[match.ID].League.MarkChanged("sport")
			m.Matches[match.ID].League.Sport.MarkChanged("name")
			status = parsed.STATUS_UPDATED
		}
		for i, participant := range match.Participants {
			if m.Matches[match.ID].Participants[i].Alignment != participant.Alignment {
				m.Matches[match.ID].Participants[i].Alignment = participant.Alignment
				m.Matches[match.ID].MarkChanged("participants")
				m.Matches[match.ID].Participants[i].MarkChanged("alignment")
				status = parsed.STATUS_UPDATED
			}
			if m.Matches[match.ID].Participants[i].Name != participant.Name {
				m.Matches[match.ID].Participants[i].Name = participant.Name
				m.Matches[match.ID].MarkChanged("participants")
				m.Matches[match.ID].Participants[i].MarkChanged("name")
				status = parsed.STATUS_UPDATED
			}
		}
		if m.Matches[match.ID].StartTime != match.StartTime {
			m.Matches[match.ID].StartTime = match.StartTime
			m.Matches[match.ID].MarkChanged("startTime")
			status = parsed.STATUS_UPDATED
		}

		m.Matches[match.ID].StatusFlag = status

		if status == parsed.STATUS_UPDATED {
			upd++
		}
	}

	m.mu.Unlock()

	if newy > 0 {
		m.MatchNewChan <- newy
	}

	if upd > 0 {
		m.MatchUpdChan <- upd
	}

	m.mu.Lock()

	deletedMatchs := make([]int, 0, 51)
	for i := range m.Matches {
		if m.Matches[i].ParentId != parentId {
			continue
		}
		if _, ok := ids[i]; ok {
			continue
		}
		deletedMatchs = append(deletedMatchs, i)
		delete(m.Matches, i)
	}

	if len(deletedMatchs) > 0 {
		for i := range deletedMatchs {
			delete(m.Bets, i)
		}
	}

	m.mu.Unlock()
	if len(deletedMatchs) > 0 {
		m.MatchDelChan <- deletedMatchs
	}

}

func (m *MapStorage) GetUpdatedMatches(n int) []*parsed.Match {
	m.mu.Lock()
	defer m.mu.Unlock()
	updatedMatches := make([]*parsed.Match, 0, n)
	for id := range m.Matches {
		if m.Matches[id].StatusFlag == parsed.STATUS_UPDATED {
			m.Matches[id].StatusFlag = parsed.STATUS_NOT_CHANGE
			updatedMatches = append(updatedMatches, m.Matches[id].GetUpdate())
		}
	}

	return updatedMatches
}

func (m *MapStorage) GetNewMatches(n int) []*parsed.Match {
	m.mu.Lock()
	defer m.mu.Unlock()

	newMatches := make([]*parsed.Match, 0, n)

	for id := range m.Matches {
		if m.Matches[id].StatusFlag == parsed.STATUS_CREATED {
			m.Matches[id].StatusFlag = parsed.STATUS_NOT_CHANGE
			newMatches = append(newMatches, m.Matches[id])
		}
	}

	return newMatches
}

func (m *MapStorage) GetUpdatedBets(n int) []*parsed.Straight {
	m.mu.Lock()
	defer m.mu.Unlock()
	updatedBets := make([]*parsed.Straight, 0, n)
	for matchId := range m.Bets {
		for betKey := range m.Bets[matchId] {
			if m.Bets[matchId][betKey].StatusFlag == parsed.STATUS_UPDATED {
				data := m.Bets[matchId][betKey].GetUpdate()
				m.Bets[matchId][betKey].StatusFlag = parsed.STATUS_NOT_CHANGE
				updatedBets = append(updatedBets, data)
			}
		}
	}

	return updatedBets
}

func (m *MapStorage) GetNewBets(n int) []*parsed.Straight {
	m.mu.Lock()
	defer m.mu.Unlock()
	newBets := make([]*parsed.Straight, 0, n)

	for matchId := range m.Bets {
		for betKey := range m.Bets[matchId] {
			if m.Bets[matchId][betKey].StatusFlag == parsed.STATUS_CREATED {
				m.Bets[matchId][betKey].StatusFlag = parsed.STATUS_NOT_CHANGE
				newBets = append(newBets, m.Bets[matchId][betKey])
			}
		}
	}

	return newBets
}

func (m *MapStorage) SetBets(bets map[int][]*parsed.Straight) {
	m.mu.Lock()
	upd := 0
	newy := 0
	for matchId := range bets {
		for _, bet := range bets[matchId] {
			_, ok := m.Bets[bet.MatchupID]
			if !ok {
				m.Bets[bet.MatchupID] = make(map[string]*parsed.Straight)
				bet.StatusFlag = parsed.STATUS_CREATED
				m.Bets[bet.MatchupID][bet.Key] = bet
				newy++
				continue
			}

			if _, ok2 := m.Bets[bet.MatchupID][bet.Key]; !ok2 {
				bet.StatusFlag = parsed.STATUS_CREATED
				m.Bets[bet.MatchupID][bet.Key] = bet
				newy++
				continue
			}

			//проверяем изменения и записываем их в мапу
			var status int8 = parsed.STATUS_NOT_CHANGE
			if m.Bets[bet.MatchupID][bet.Key].Period != bet.Period {
				m.Bets[bet.MatchupID][bet.Key].Period = bet.Period
				m.Bets[bet.MatchupID][bet.Key].MarkChanged("period")
				status = parsed.STATUS_UPDATED
			}
			if m.Bets[bet.MatchupID][bet.Key].Side != bet.Side {
				m.Bets[bet.MatchupID][bet.Key].Side = bet.Side
				m.Bets[bet.MatchupID][bet.Key].MarkChanged("side")
				status = parsed.STATUS_UPDATED
			}
			if m.Bets[bet.MatchupID][bet.Key].Status != bet.Status {
				m.Bets[bet.MatchupID][bet.Key].Status = bet.Status
				m.Bets[bet.MatchupID][bet.Key].MarkChanged("status")
				status = parsed.STATUS_UPDATED
			}
			if m.Bets[bet.MatchupID][bet.Key].Type != bet.Type {
				m.Bets[bet.MatchupID][bet.Key].Type = bet.Type
				m.Bets[bet.MatchupID][bet.Key].MarkChanged("type")
				status = parsed.STATUS_UPDATED
			}
			for i, price := range bet.Prices {
				if i >= len(m.Bets[bet.MatchupID][bet.Key].Prices) {
					continue
				}
				if m.Bets[bet.MatchupID][bet.Key].Prices[i].Designation != price.Designation {
					m.Bets[bet.MatchupID][bet.Key].Prices[i].Designation = price.Designation
					m.Bets[bet.MatchupID][bet.Key].MarkChanged("prices")
					m.Bets[bet.MatchupID][bet.Key].Prices[i].MarkChanged("designation")
					status = parsed.STATUS_UPDATED
				}
				if m.Bets[bet.MatchupID][bet.Key].Prices[i].Price != price.Price {
					m.Bets[bet.MatchupID][bet.Key].Prices[i].Price = price.Price
					m.Bets[bet.MatchupID][bet.Key].MarkChanged("prices")
					m.Bets[bet.MatchupID][bet.Key].Prices[i].MarkChanged("price")
					status = parsed.STATUS_UPDATED
				}
				if m.Bets[bet.MatchupID][bet.Key].Prices[i].Points != price.Points {
					m.Bets[bet.MatchupID][bet.Key].Prices[i].Points = price.Points
					m.Bets[bet.MatchupID][bet.Key].MarkChanged("prices")
					m.Bets[bet.MatchupID][bet.Key].Prices[i].MarkChanged("points")
					status = parsed.STATUS_UPDATED
				}

				m.Bets[bet.MatchupID][bet.Key].StatusFlag = status

				if status == parsed.STATUS_UPDATED {
					upd++
				}
			}
		}
	}

	m.mu.Unlock()

	if newy > 0 {
		m.BetNewChan <- newy
	}

	if upd > 0 {
		m.BetUpdChan <- upd
	}
}
