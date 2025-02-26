package parsed

import (
	"fmt"
	"math/rand"
)

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

// Example bet types and their descriptions
var exampleBetTypes = []struct {
	betType     string
	hasPoints   bool
	description string
}{
	{"moneyline", false, "Simple win/lose bet"},
	{"spread", true, "Point spread bet"},
	{"total", true, "Over/under total points bet"},
}

// GenerateExampleStraight creates a new Straight instance with random example data
func GenerateExampleStraight(matchID int) *Straight {
	betType := exampleBetTypes[rand.Intn(len(exampleBetTypes))]

	straight := &Straight{
		Key:        fmt.Sprintf("%d-%s-%d", matchID, betType.betType, rand.Int()),
		MatchupID:  matchID,
		Period:     rand.Intn(4) + 1, // Periods 1-4 for quarters/periods
		Side:       []string{"over", "under", "home", "away"}[rand.Intn(4)],
		Status:     []string{"open", "suspended", "closed"}[rand.Intn(3)],
		Type:       betType.betType,
		Prices:     make([]*Price, 0, 2),
		StatusFlag: STATUS_CREATED,
	}

	numPrices := 2

	for i := 0; i < numPrices; i++ {
		price := &Price{
			ParticipantId: i + 1,
			Price:         (rand.Intn(800) + 100) * ([]int{-1, 1}[rand.Intn(2)]), // Random price between -800 and +800
			Designation:   []string{"home", "away", "over", "under"}[rand.Intn(4)],
		}

		if betType.hasPoints {
			// Generate points for spread or totals
			basePoints := 0
			switch betType.betType {
			case "spread":
				basePoints = rand.Intn(20) + 1 // 1-20 point spread
			case "total":
				basePoints = rand.Intn(150) + 100 // 100-250 total points (basketball/football)
			}
			price.Points = float64(basePoints) + float64(rand.Intn(2))*0.5 // Add .0 or .5
		}

		straight.Prices = append(straight.Prices, price)
	}

	return straight
}

// GenerateRandomStraightDelta creates random changes for an existing straight bet
func GenerateRandomStraightDelta(straight *Straight) *Straight {
	// Create a copy with only the key identifiers
	delta := &Straight{
		Key:        straight.Key,
		MatchupID:  straight.MatchupID,
		Type:       straight.Type,
		StatusFlag: STATUS_UPDATED,
		Prices:     make([]*Price, len(straight.Prices)),
	}

	// 40% chance to change status
	if rand.Float32() < 0.4 {
		newStatus := []string{"open", "suspended", "closed"}[rand.Intn(3)]
		if newStatus != straight.Status {
			delta.Status = newStatus
			delta.MarkChanged("status")
		}
	}

	// Always modify at least one price (since this is the most common change)
	priceChanged := false
	for i, oldPrice := range straight.Prices {
		delta.Prices[i] = &Price{
			ParticipantId: oldPrice.ParticipantId,
			Designation:   oldPrice.Designation,
		}

		// 80% chance to change the actual price
		if rand.Float32() < 0.8 {
			// Generate new price with small variation
			priceChange := (rand.Intn(50) + 1) * ([]int{-1, 1}[rand.Intn(2)])
			newPrice := oldPrice.Price + priceChange
			if newPrice != oldPrice.Price {
				delta.Prices[i].Price = newPrice
				delta.Prices[i].MarkChanged("price")
				delta.MarkChanged("prices")
				priceChanged = true
			}
		}

		// If it's a spread or total bet, 30% chance to change points
		if oldPrice.Points != 0 && rand.Float32() < 0.3 {
			pointChange := float64(rand.Intn(2)) * 0.5 * float64([]int{-1, 1}[rand.Intn(2)])
			newPoints := oldPrice.Points + pointChange
			if newPoints != oldPrice.Points {
				delta.Prices[i].Points = newPoints
				delta.Prices[i].MarkChanged("points")
				delta.MarkChanged("prices")
				priceChanged = true
			}
		}
	}

	// If no changes were made, force at least one price change
	if !priceChanged {
		i := rand.Intn(len(straight.Prices))
		priceChange := (rand.Intn(50) + 1) * ([]int{-1, 1}[rand.Intn(2)])
		delta.Prices[i].Price = straight.Prices[i].Price + priceChange
		delta.Prices[i].MarkChanged("price")
		delta.MarkChanged("prices")
	}

	return delta
}
