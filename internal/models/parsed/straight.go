package parsed

type Price struct {
	Designation   string          `json:"designation,omitempty"`
	Price         int             `json:"price,omitempty"`
	Points        float64         `json:"points,omitempty"`
	ParticipantId int             `json:"participantId,omitempty"`
	Changes       map[string]bool `json:"-"`
}

func (p *Price) MarkChanged(field string) {
	if p.Changes == nil {
		p.Changes = make(map[string]bool, 3)
	}
	p.Changes[field] = true
}

type Straight struct {
	Key        string          `json:"key,omitempty"`
	MatchupID  int             `json:"matchupId,omitempty"`
	Period     int             `json:"period,omitempty"`
	Prices     []*Price        `json:"prices,omitempty"`
	Side       string          `json:"side,omitempty"`
	Status     string          `json:"status,omitempty"`
	Type       string          `json:"type,omitempty"`
	StatusFlag int8            `json:"-"`
	Changes    map[string]bool `json:"-"`
}

func (s *Straight) MarkChanged(field string) {
	if s.Changes == nil {
		s.Changes = make(map[string]bool, 7)
	}
	s.Changes[field] = true
}

func (s *Straight) GetUpdate() *Straight {
	if len(s.Changes) == 0 {
		return nil
	}
	upd := &Straight{}
	upd.MatchupID = s.MatchupID
	upd.Key = s.Key
	upd.Type = s.Type
	for field := range s.Changes {
		if field == "period" {
			upd.Period = s.Period
			continue
		}
		if field == "prices" {
			upd.Prices = make([]*Price, 0, len(s.Prices))
			for _, p := range s.Prices {
				if p.Changes == nil {
					continue
				}
				price := &Price{}
				if p.ParticipantId != 0 {
					price.ParticipantId = p.ParticipantId
				}
				if p.Designation != "" {
					price.Designation = p.Designation
				}
				for field := range p.Changes {
					if field == "price" {
						price.Price = p.Price
						continue
					}
					if field == "points" {
						price.Points = p.Points
						continue
					}
				}
				upd.Prices = append(upd.Prices, price)
			}
		}
		if field == "side" {
			upd.Side = s.Side
			continue
		}
		if field == "status" {
			upd.Status = s.Status
			continue
		}
	}

	return upd
}
