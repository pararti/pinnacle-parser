package test

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pararti/pinnacle-parser/internal/models/parsed"
)

// StraightTypes хранит возможные типы ставок
var StraightTypes = []string{"moneyline", "spread", "total", "team_total"}

// StraightDesignations хранит возможные обозначения для цен
var StraightDesignations = []string{"home", "away", "over", "under"}

// StraightStatuses хранит возможные статусы ставок
var StraightStatuses = []string{"open", "suspended", "closed"}

// StraightSides хранит возможные стороны для ставок
var StraightSides = []string{"home", "away", "over", "under"}

// Периоды для ставок, где 0 - весь матч
var StraightPeriods = []int{0, 1, 2, 3}

// GenerateExampleStraights генерирует несколько примеров ставок для заданного матча
// Если isParticipantWithId = true, то создаем prices с participantId, иначе с designation
func GenerateExampleStraights(matchID int, isParticipantWithId bool, participantIds []int) []*parsed.Straight {
	straightsList := make([]*parsed.Straight, 0)

	// Если isParticipantWithId = true и передан пустой массив ids, то генерируем тестовые ID
	if isParticipantWithId && (participantIds == nil || len(participantIds) == 0) {
		participantIds = []int{
			1000000 + rand.Intn(1000000),
			1000000 + rand.Intn(1000000),
			1000000 + rand.Intn(1000000),
			1000000 + rand.Intn(1000000),
		}
	}

	// Для participants с id (alignment="neutral") генерируем только moneyline для всего матча
	if isParticipantWithId {
		straight := &parsed.Straight{
			Key:        "s;0;m",
			MatchupID:  matchID,
			Period:     0,
			Status:     "open",
			Type:       "moneyline",
			StatusFlag: parsed.STATUS_CREATED,
			Prices:     make([]*parsed.Price, 0, len(participantIds)),
		}

		// Генерируем цены для каждого participant по его ID
		for _, participantId := range participantIds {
			price := &parsed.Price{
				ParticipantId: participantId,
				Price:         generateRandomPrice(200, 400), // Положительные коэффициенты для special ставок
			}
			price.MarkChanged("price")
			straight.Prices = append(straight.Prices, price)
		}

		straight.MarkChanged("prices")
		straightsList = append(straightsList, straight)

		return straightsList
	}

	// Далее код для обычных участников (home/away)

	// Генерация moneyline для всех периодов
	for _, period := range StraightPeriods {
		straight := &parsed.Straight{
			Key:        fmt.Sprintf("s;%d;m", period),
			MatchupID:  matchID,
			Period:     period,
			Status:     "open",
			Type:       "moneyline",
			StatusFlag: parsed.STATUS_CREATED,
			Prices:     make([]*parsed.Price, 0, 2),
		}

		// Цены для home и away
		homePrice := &parsed.Price{
			Designation: "home",
			Price:       generateRandomPrice(-1200, 500),
		}
		homePrice.MarkChanged("price")

		awayPrice := &parsed.Price{
			Designation: "away",
			Price:       generateRandomPrice(-300, 400),
		}
		awayPrice.MarkChanged("price")

		straight.Prices = append(straight.Prices, homePrice, awayPrice)
		straight.MarkChanged("prices")

		straightsList = append(straightsList, straight)
	}

	// Генерация spread для всего матча
	spreadStraight := &parsed.Straight{
		Key:        "s;0;s;1.5",
		MatchupID:  matchID,
		Period:     0,
		Status:     "open",
		Type:       "spread",
		StatusFlag: parsed.STATUS_CREATED,
		Prices:     make([]*parsed.Price, 0, 2),
	}

	homeSpreadPrice := &parsed.Price{
		Designation: "home",
		Price:       generateRandomPrice(-200, 200),
		Points:      1.5,
	}
	homeSpreadPrice.MarkChanged("price")
	homeSpreadPrice.MarkChanged("points")

	awaySpreadPrice := &parsed.Price{
		Designation: "away",
		Price:       generateRandomPrice(-200, 200),
		Points:      -1.5,
	}
	awaySpreadPrice.MarkChanged("price")
	awaySpreadPrice.MarkChanged("points")

	spreadStraight.Prices = append(spreadStraight.Prices, homeSpreadPrice, awaySpreadPrice)
	spreadStraight.MarkChanged("prices")

	straightsList = append(straightsList, spreadStraight)

	// Генерация total для всего матча
	totalStraight := &parsed.Straight{
		Key:        "s;0;ou;2.5",
		MatchupID:  matchID,
		Period:     0,
		Status:     "open",
		Type:       "total",
		StatusFlag: parsed.STATUS_CREATED,
		Prices:     make([]*parsed.Price, 0, 2),
	}

	overTotalPrice := &parsed.Price{
		Designation: "over",
		Price:       generateRandomPrice(-200, 200),
		Points:      2.5,
	}
	overTotalPrice.MarkChanged("price")
	overTotalPrice.MarkChanged("points")

	underTotalPrice := &parsed.Price{
		Designation: "under",
		Price:       generateRandomPrice(-200, 200),
		Points:      2.5,
	}
	underTotalPrice.MarkChanged("price")
	underTotalPrice.MarkChanged("points")

	totalStraight.Prices = append(totalStraight.Prices, overTotalPrice, underTotalPrice)
	totalStraight.MarkChanged("prices")

	straightsList = append(straightsList, totalStraight)

	// Иногда добавляем team_total для первого периода
	if rand.Intn(2) == 1 {
		teamTotalStraight := &parsed.Straight{
			Key:        "s;1;tt;10.5;home",
			MatchupID:  matchID,
			Period:     1,
			Side:       "home",
			Status:     "open",
			Type:       "team_total",
			StatusFlag: parsed.STATUS_CREATED,
			Prices:     make([]*parsed.Price, 0, 2),
		}

		overTeamTotalPrice := &parsed.Price{
			Designation: "over",
			Price:       generateRandomPrice(-200, 200),
			Points:      10.5,
		}
		overTeamTotalPrice.MarkChanged("price")
		overTeamTotalPrice.MarkChanged("points")

		underTeamTotalPrice := &parsed.Price{
			Designation: "under",
			Price:       generateRandomPrice(-200, 200),
			Points:      10.5,
		}
		underTeamTotalPrice.MarkChanged("price")
		underTeamTotalPrice.MarkChanged("points")

		teamTotalStraight.Prices = append(teamTotalStraight.Prices, overTeamTotalPrice, underTeamTotalPrice)
		teamTotalStraight.MarkChanged("prices")

		straightsList = append(straightsList, teamTotalStraight)
	}

	return straightsList
}

// GenerateRandomStraightDeltas генерирует случайные изменения для существующих ставок
func GenerateRandomStraightDeltas(straights map[string]*parsed.Straight, isParticipantWithId bool, participantIds []int) map[string]*parsed.Straight {
	// Проверяем входные данные
	if straights == nil || len(straights) == 0 {
		return make(map[string]*parsed.Straight)
	}

	// Выберем случайное количество ставок для обновления (от 1 до 3)
	updateCount := rand.Intn(3) + 1
	updatedStraights := make(map[string]*parsed.Straight)

	// Преобразуем map в срез ключей для случайного выбора
	keys := make([]string, 0, len(straights))
	for k, straight := range straights {
		if straight != nil && k != "" {
			keys = append(keys, k)
		}
	}

	// Если нет допустимых ставок, возвращаем пустой результат
	if len(keys) == 0 {
		return updatedStraights
	}

	// Перемешаем ключи
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	// Обновим случайные ставки
	for i := 0; i < updateCount && i < len(keys); i++ {
		straight := straights[keys[i]]
		if straight == nil {
			continue
		}

		delta := parsed.GenerateRandomStraightDelta(straight)
		if delta != nil {
			delta.StatusFlag = parsed.STATUS_UPDATED
			updatedStraights[delta.Key] = delta
		}
	}

	// Иногда добавляем новую ставку (с вероятностью 20%)
	if rand.Float32() < 0.2 && len(straights) > 0 {
		// Берем ID матча из существующей ставки
		var matchID int
		for _, s := range straights {
			if s != nil {
				matchID = s.MatchupID
				break
			}
		}

		// Генерируем новую ставку
		newStraight := generateNewStraight(matchID, isParticipantWithId, participantIds)
		if newStraight != nil {
			newStraight.StatusFlag = parsed.STATUS_CREATED
			updatedStraights[newStraight.Key] = newStraight
		}
	}

	// Иногда удаляем ставку (с вероятностью 10%)
	if rand.Float32() < 0.1 && len(keys) > 2 {
		keyToDelete := keys[len(keys)-1]
		straight := straights[keyToDelete]
		if straight != nil {
			deletedStraight := *straight
			deletedStraight.StatusFlag = parsed.STATUS_DELETED
			updatedStraights[keyToDelete] = &deletedStraight
		}
	}

	return updatedStraights
}

// generateNewStraight создает новую уникальную ставку для указанного матча
// С учетом типа participants (с id или без)
func generateNewStraight(matchID int, isParticipantWithId bool, participantIds []int) *parsed.Straight {
	// Для матчей со special participants (с ID) генерируем только moneyline
	if isParticipantWithId {
		key := "s;0;m"

		straight := &parsed.Straight{
			Key:        key,
			MatchupID:  matchID,
			Period:     0,
			Status:     "open",
			Type:       "moneyline",
			StatusFlag: parsed.STATUS_CREATED,
			Prices:     make([]*parsed.Price, 0, len(participantIds)),
		}

		// Генерируем цены для каждого participant по его ID
		for _, participantId := range participantIds {
			price := &parsed.Price{
				ParticipantId: participantId,
				Price:         generateRandomPrice(200, 400), // Положительные коэффициенты для special ставок
			}
			price.MarkChanged("price")
			straight.Prices = append(straight.Prices, price)
		}

		straight.MarkChanged("prices")
		return straight
	}

	// Далее код для обычных матчей с home/away participants
	straightType := StraightTypes[rand.Intn(len(StraightTypes))]
	period := StraightPeriods[rand.Intn(len(StraightPeriods))]

	var key string
	var straight *parsed.Straight

	switch straightType {
	case "spread":
		points := float64(rand.Intn(5)+1) - 0.5 // Генерируем points от 0.5 до 4.5
		key = fmt.Sprintf("s;%d;s;%.1f", period, points)

		straight = &parsed.Straight{
			Key:        key,
			MatchupID:  matchID,
			Period:     period,
			Status:     "open",
			Type:       "spread",
			StatusFlag: parsed.STATUS_CREATED,
			Prices:     make([]*parsed.Price, 0, 2),
		}

		homePrice := &parsed.Price{
			Designation: "home",
			Price:       generateRandomPrice(-200, 200),
			Points:      points,
		}
		homePrice.MarkChanged("price")
		homePrice.MarkChanged("points")

		awayPrice := &parsed.Price{
			Designation: "away",
			Price:       generateRandomPrice(-200, 200),
			Points:      -points,
		}
		awayPrice.MarkChanged("price")
		awayPrice.MarkChanged("points")

		straight.Prices = append(straight.Prices, homePrice, awayPrice)

	case "total":
		points := float64(rand.Intn(5)+1) + 0.5 // Генерируем points от 1.5 до 5.5
		key = fmt.Sprintf("s;%d;ou;%.1f", period, points)

		straight = &parsed.Straight{
			Key:        key,
			MatchupID:  matchID,
			Period:     period,
			Status:     "open",
			Type:       "total",
			StatusFlag: parsed.STATUS_CREATED,
			Prices:     make([]*parsed.Price, 0, 2),
		}

		overPrice := &parsed.Price{
			Designation: "over",
			Price:       generateRandomPrice(-200, 200),
			Points:      points,
		}
		overPrice.MarkChanged("price")
		overPrice.MarkChanged("points")

		underPrice := &parsed.Price{
			Designation: "under",
			Price:       generateRandomPrice(-200, 200),
			Points:      points,
		}
		underPrice.MarkChanged("price")
		underPrice.MarkChanged("points")

		straight.Prices = append(straight.Prices, overPrice, underPrice)

	case "team_total":
		points := float64(rand.Intn(15)+5) + 0.5 // Генерируем points от 5.5 до 19.5
		side := StraightSides[rand.Intn(2)]      // Используем только "home" или "away"
		key = fmt.Sprintf("s;%d;tt;%.1f;%s", period, points, side)

		straight = &parsed.Straight{
			Key:        key,
			MatchupID:  matchID,
			Period:     period,
			Side:       side,
			Status:     "open",
			Type:       "team_total",
			StatusFlag: parsed.STATUS_CREATED,
			Prices:     make([]*parsed.Price, 0, 2),
		}

		overPrice := &parsed.Price{
			Designation: "over",
			Price:       generateRandomPrice(-200, 200),
			Points:      points,
		}
		overPrice.MarkChanged("price")
		overPrice.MarkChanged("points")

		underPrice := &parsed.Price{
			Designation: "under",
			Price:       generateRandomPrice(-200, 200),
			Points:      points,
		}
		underPrice.MarkChanged("price")
		underPrice.MarkChanged("points")

		straight.Prices = append(straight.Prices, overPrice, underPrice)

	default: // moneyline
		key = fmt.Sprintf("s;%d;m", period)

		straight = &parsed.Straight{
			Key:        key,
			MatchupID:  matchID,
			Period:     period,
			Status:     "open",
			Type:       "moneyline",
			StatusFlag: parsed.STATUS_CREATED,
			Prices:     make([]*parsed.Price, 0, 2),
		}

		homePrice := &parsed.Price{
			Designation: "home",
			Price:       generateRandomPrice(-1200, 500),
		}
		homePrice.MarkChanged("price")

		awayPrice := &parsed.Price{
			Designation: "away",
			Price:       generateRandomPrice(-300, 400),
		}
		awayPrice.MarkChanged("price")

		straight.Prices = append(straight.Prices, homePrice, awayPrice)
	}

	straight.MarkChanged("prices")
	return straight
}

// generateRandomPrice генерирует случайную цену в американском формате в указанном диапазоне
func generateRandomPrice(min, max int) int {
	// Генерируем случайную цену
	price := rand.Intn(max-min+1) + min

	// Если цена близка к нулю, сдвигаем ее дальше от нуля
	if price > -100 && price < 100 {
		if price >= 0 {
			price += 100
		} else {
			price -= 100
		}
	}

	return price
}

// Инициализация генератора случайных чисел
func init() {
	rand.Seed(time.Now().UnixNano())
}
