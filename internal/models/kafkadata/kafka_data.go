package kafkadata

import (
	"github.com/pararti/pinnacle-parser/internal/models/parsed"
)

type Match struct {
	EventType int             `json:"eventType"`
	Source    string          `json:"source"`
	Data      []*parsed.Match `json:"data"`
}

type MatchUpd struct {
	EventType int             `json:"eventType"`
	Source    string          `json:"source"`
	Data      []*parsed.Match `json:"data"`
}

type Bet struct {
	EventType int                `json:"eventType"`
	Source    string             `json:"source"`
	Data      []*parsed.Straight `json:"data"`
}

type BetUpd struct {
	EventType int                `json:"eventType"`
	Source    string             `json:"source"`
	Data      []*parsed.Straight `json:"data"`
}

type DeletedMatch struct {
	EventType int    `json:"eventType"`
	Source    string `json:"source"`
	Data      []int  `json:"data"`
}
