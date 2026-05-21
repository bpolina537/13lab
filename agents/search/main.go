package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
)

// SearchRequest — входящий запрос на поиск мест
type SearchRequest struct {
	Zone string `json:"zone"`
}

// SearchResult — результат поиска мест
type SearchResult struct {
	Zone   string   `json:"zone"`
	Places []string `json:"places"`
}

// CompletedEvent — публикуется в tasks.completed после обработки
type CompletedEvent struct {
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

// zoneMap — фиксированный список мест по зонам
var zoneMap = map[string][]string{
	"A": {"A1", "A2", "A3"},
	"B": {"B1", "B2"},
	"C": {"C1", "C2", "C3", "C4"},
}

func main() {
	logger := log.New(os.Stdout, "[SEARCH-AGENT] ", log.LstdFlags|log.Lmsgprefix)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL,
		nats.Name("parking-search-agent"),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Printf("Отключён от NATS: %v", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			logger.Println("Переподключён к NATS")
		}),
	)
	if err != nil {
		logger.Fatalf("Ошибка подключения к NATS: %v", err)
	}
	defer nc.Drain()

	logger.Printf("Подключён к NATS: %s", natsURL)

	sub, err := nc.Subscribe("parking.search", func(msg *nats.Msg) {
		logger.Printf("Получено сообщение на канале %s: %s", msg.Subject, string(msg.Data))

		var req SearchRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			logger.Printf("Ошибка парсинга запроса: %v", err)
			return
		}

		if req.Zone == "" {
			logger.Println("Ошибка: поле 'zone' не задано")
			return
		}

		places, ok := zoneMap[req.Zone]
		if !ok {
			logger.Printf("Зона '%s' не найдена", req.Zone)
			places = []string{}
		}

		result := SearchResult{
			Zone:   req.Zone,
			Places: places,
		}
		logger.Printf("Найдено мест в зоне %s: %v", req.Zone, places)

		event := CompletedEvent{
			Agent:   "search",
			Subject: msg.Subject,
			Result:  result,
		}
		payload, err := json.Marshal(event)
		if err != nil {
			logger.Printf("Ошибка сериализации результата: %v", err)
			return
		}

		if err := nc.Publish("tasks.completed", payload); err != nil {
			logger.Printf("Ошибка публикации в tasks.completed: %v", err)
			return
		}
		logger.Printf("Результат опубликован в tasks.completed: %s", string(payload))
	})
	if err != nil {
		logger.Fatalf("Ошибка подписки на parking.search: %v", err)
	}
	defer sub.Unsubscribe()

	logger.Println("Агент запущен, ожидание сообщений на канале parking.search...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Println("Завершение работы агента поиска")
}
