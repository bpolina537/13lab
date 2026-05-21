package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/nats-io/nats.go"
)

type TaskEnvelope struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type AccessRequest struct {
	BookingID string `json:"booking_id"`
	CarNumber string `json:"car_number"`
	Action    string `json:"action"`
}

type AccessResult struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	BookingID string `json:"booking_id"`
}

type CompletedEvent struct {
	TaskID  string      `json:"task_id"`
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

type AccessStore struct {
	mu       sync.RWMutex
	bookings map[string]struct {
		CarNumber string
		IsInside  bool
	}
}

func NewAccessStore() *AccessStore {
	return &AccessStore{
		bookings: make(map[string]struct {
			CarNumber string
			IsInside  bool
		}),
	}
}

func (s *AccessStore) CheckAccess(bookingID, carNumber, action string) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, ok := s.bookings[bookingID]
	if !ok {
		// Авторегистрация при первом запросе
		s.bookings[bookingID] = struct {
			CarNumber string
			IsInside  bool
		}{carNumber, false}
		info = s.bookings[bookingID]
	}

	if info.CarNumber != carNumber {
		return false, "Номер автомобиля не совпадает"
	}

	switch action {
	case "enter":
		if info.IsInside {
			return false, "Автомобиль уже на парковке"
		}
		info.IsInside = true
		s.bookings[bookingID] = info
		return true, "Въезд разрешён"
	case "exit":
		if !info.IsInside {
			return false, "Автомобиль не на парковке"
		}
		info.IsInside = false
		s.bookings[bookingID] = info
		return true, "Выезд разрешён"
	default:
		return false, "Неизвестное действие"
	}
}

func main() {
	logger := log.New(os.Stdout, "[ACCESS-AGENT] ", log.LstdFlags|log.Lmsgprefix)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Fatalf("Ошибка подключения: %v", err)
	}
	defer nc.Drain()

	logger.Printf("Подключён к NATS: %s", natsURL)

	store := NewAccessStore()

	sub, err := nc.Subscribe("parking.access", func(msg *nats.Msg) {
		logger.Printf("Получено сообщение: %s", string(msg.Data))

		// Парсим конверт от оркестратора
		var envelope TaskEnvelope
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			logger.Printf("Ошибка парсинга конверта: %v", err)
			return
		}

		// Парсим payload
		var req AccessRequest
		if err := json.Unmarshal([]byte(envelope.Payload), &req); err != nil {
			logger.Printf("Ошибка парсинга payload: %v", err)
			return
		}

		logger.Printf("Запрос: booking_id=%s, car=%s, action=%s", req.BookingID, req.CarNumber, req.Action)

		success, message := store.CheckAccess(req.BookingID, req.CarNumber, req.Action)

		result := AccessResult{
			Success:   success,
			Message:   message,
			Action:    req.Action,
			BookingID: req.BookingID,
		}

		if success {
			logger.Printf("✅ Доступ разрешён: %s", message)
		} else {
			logger.Printf("❌ Доступ отклонён: %s", message)
		}

		event := CompletedEvent{
			TaskID:  envelope.ID,
			Agent:   "access",
			Subject: msg.Subject,
			Result:  result,
		}
		payload, _ := json.Marshal(event)
		nc.Publish("tasks.completed", payload)
		logger.Printf("Ответ отправлен (task_id=%s)", envelope.ID)
	})
	if err != nil {
		logger.Fatalf("Ошибка подписки: %v", err)
	}
	defer sub.Unsubscribe()

	logger.Println("Агент доступа запущен, ожидание сообщений...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Println("Завершение работы")
}