package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
)

// ЭТИ СТРУКТУРЫ ДОЛЖНЫ ПОЛНОСТЬЮ СООТВЕТСТВОВАТЬ ТОМУ, ЧТО ШЛЕТ ОРКЕСТРАТОР
type TaskEnvelope struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type SearchRequest struct {
	Zone string `json:"zone"`
}

// ЭТОТ РЕЗУЛЬТАТ УВИДИТ ОРКЕСТРАТОР
type SearchResult struct {
	Zone   string   `json:"zone"`
	Places []string `json:"places"`
}

// ЭТО СОБЫТИЕ, КОТОРОЕ ОРКЕСТРАТОР ЖДЕТ В КАНАЛЕ tasks.completed
type CompletedEvent struct {
    TaskID  string      `json:"task_id"`
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

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

	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Fatalf("Ошибка подключения к NATS: %v", err)
	}
	defer nc.Drain()

	logger.Printf("Подключён к NATS: %s", natsURL)

	sub, err := nc.Subscribe("parking.search", func(msg *nats.Msg) {
		// 1. ПРИНИМАЕМ КОНВЕРТ ОТ ОРКЕСТРАТОРА
		var envelope TaskEnvelope
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			logger.Printf("Ошибка парсинга конверта: %v. Данные: %s", err, string(msg.Data))
			return
		}
		logger.Printf("Получен конверт: ID=%s, Type=%s", envelope.ID, envelope.Type)

		// 2. ДОСТАЕМ РЕАЛЬНЫЙ ЗАПРОС ИЗ PAYLOAD
		var req SearchRequest
		if err := json.Unmarshal([]byte(envelope.Payload), &req); err != nil {
			logger.Printf("Ошибка парсинга payload: %v. Payload: %s", err, envelope.Payload)
			return
		}
		logger.Printf("Поиск в зоне: %s", req.Zone)

		// 3. ФОРМИРУЕМ РЕЗУЛЬТАТ
		places, ok := zoneMap[req.Zone]
		if !ok {
			places = []string{}
		}
		result := SearchResult{Zone: req.Zone, Places: places}
		logger.Printf("Результат поиска: %v", places)

		// 4. ОТПРАВЛЯЕМ ОТВЕТ В ФОРМАТЕ, КОТОРЫЙ ЖДЕТ ОРКЕСТРАТОР
		event := CompletedEvent{
		    TaskID:  envelope.ID,
			Agent:   "search",
			Subject: msg.Subject,
			Result:  result,
		}
		payload, err := json.Marshal(event)
		if err != nil {
			logger.Printf("Ошибка сериализации ответа: %v", err)
			return
		}
		if err := nc.Publish("tasks.completed", payload); err != nil {
			logger.Printf("Ошибка публикации ответа: %v", err)
			return
		}
		logger.Printf("Ответ отправлен в tasks.completed")
	})
	if err != nil {
		logger.Fatalf("Ошибка подписки: %v", err)
	}
	defer sub.Unsubscribe()

	logger.Println("Агент поиска запущен и ждет сообщения...")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Println("Завершение работы")
}