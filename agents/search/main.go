package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

type TaskEnvelope struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type SearchRequest struct {
	Zone string `json:"zone"`
}

type SearchResult struct {
	Zone   string   `json:"zone"`
	Places []string `json:"places"`
}

type CompletedEvent struct {
	TaskID  string      `json:"task_id"`
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

type AuctionRequest struct {
	TaskID string       `json:"task_id"`
	Task   TaskEnvelope `json:"task"`
}

type AuctionBid struct {
	TaskID    string `json:"task_id"`
	AgentID   string `json:"agent_id"`
	AgentZone string `json:"agent_zone"`
	Bid       int    `json:"bid"`
	Load      int    `json:"load"`
}

type AuctionWinner struct {
	TaskID   string       `json:"task_id"`
	AgentID  string       `json:"agent_id"`
	Task     TaskEnvelope `json:"task"`
}

var zoneMap = map[string][]string{
	"A": {"A1", "A2", "A3"},
	"B": {"B1", "B2"},
	"C": {"C1", "C2", "C3", "C4"},
}

var agentID string
var agentZone string
var taskCount int32

func calculateBid(zone string) int {
	// Базовая цена
	basePrice := 10

	// Загруженность (чем больше задач, тем дороже)
	load := atomic.LoadInt32(&taskCount)
	loadCost := int(load) * 5

	// Специализация: если зона агента совпадает с запрошенной — скидка
	skillMatch := 0
	if agentZone == zone {
		skillMatch = 10
	}

	// Доступность: если занят > 3 задач — штраф
	availabilityPenalty := 0
	if load > 3 {
		availabilityPenalty = 20
	}

	// Итоговая цена
	price := basePrice + loadCost - skillMatch + availabilityPenalty
	if price < 0 {
		price = 0
	}

	return price
}

func main() {
	logger := log.New(os.Stdout, "[SEARCH-AGENT] ", log.LstdFlags|log.Lmsgprefix)

	// Генерируем уникальный ID и зону агента
	agentID = os.Getenv("HOSTNAME")
	if agentID == "" {
		agentID = "search-" + time.Now().Format("150405")
	}

	// Агент получает случайную специализацию (зона A, B или C)
	zones := []string{"A", "B", "C"}
	agentZone = zones[rand.Intn(3)]
	logger.Printf("Agent ID: %s, специализация: зона %s", agentID, agentZone)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Fatalf("Ошибка: %v", err)
	}
	defer nc.Drain()

	// Подписка на аукционные запросы
	auctionSub, _ := nc.Subscribe("auction.search.request", func(msg *nats.Msg) {
		var req AuctionRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			logger.Printf("Ошибка парсинга аукциона: %v", err)
			return
		}

		// Парсим payload для получения зоны
		var searchReq SearchRequest
		if err := json.Unmarshal([]byte(req.Task.Payload), &searchReq); err != nil {
			logger.Printf("Ошибка payload: %v", err)
			return
		}

		// Вычисляем цену
		price := calculateBid(searchReq.Zone)

		logger.Printf("Аукцион task=%s, зона=%s, цена=%d (нагрузка=%d)",
			req.TaskID, searchReq.Zone, price, taskCount)

		bid := AuctionBid{
			TaskID:    req.TaskID,
			AgentID:   agentID,
			AgentZone: agentZone,
			Bid:       price,
			Load:      int(taskCount),
		}
		payload, _ := json.Marshal(bid)
		nc.Publish("auction.search.bid", payload)
	})

	// Подписка на победителя аукциона
	winnerSub, _ := nc.Subscribe("auction.search.winner", func(msg *nats.Msg) {
		var winner AuctionWinner
		if err := json.Unmarshal(msg.Data, &winner); err != nil {
			logger.Printf("Ошибка: %v", err)
			return
		}

		if winner.AgentID != agentID {
			return
		}

		// Увеличиваем счётчик задач
		atomic.AddInt32(&taskCount, 1)
		defer atomic.AddInt32(&taskCount, -1)

		logger.Printf("🏆 Победил в аукционе! Обрабатываю задачу %s", winner.TaskID)

		// Обработка задачи
		var req SearchRequest
		if err := json.Unmarshal([]byte(winner.Task.Payload), &req); err != nil {
			logger.Printf("Ошибка payload: %v", err)
			return
		}

		time.Sleep(100 * time.Millisecond) // симуляция работы

		places, ok := zoneMap[req.Zone]
		if !ok {
			places = []string{}
		}

		result := SearchResult{Zone: req.Zone, Places: places}
		event := CompletedEvent{
			TaskID:  winner.TaskID,
			Agent:   "search",
			Subject: winner.Task.Type,
			Result:  result,
		}
		payload, _ := json.Marshal(event)
		nc.Publish("tasks.completed", payload)
		logger.Printf("✅ Результат отправлен, мест: %d", len(places))
	})

	defer auctionSub.Unsubscribe()
	defer winnerSub.Unsubscribe()

	logger.Println("Агент поиска запущен (с аукционом)")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Println("Завершение")
}