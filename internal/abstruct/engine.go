package abstruct

import "github.com/pararti/pinnacle-parser/internal/options"

type Engine interface {
	Start(*options.Options)
}
