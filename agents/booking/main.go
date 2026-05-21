package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

type TaskEnvelope struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type BookingRequest struct {
	PlaceID   string `json:"place_id"`
	CarNumber string `json:"car_number"`
	Hours     int    `json:"hours"`
}

// В BookingResult НЕТ task_id
type BookingResult struct {
	BookingID string `json:"booking_id"`
	Success   bool   `json:"success"`
	Message   string `json:"message,omitempty"`
}

type CompletedEvent struct {
	TaskID  string      `json:"task_id"`   // <-- task_id ТОЛЬКО ЗДЕСЬ
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

type BookingStore struct {
	mu       sync.RWMutex
	bookings map[string]interface{}
	places   map[string]string
}

func NewBookingStore() *BookingStore {
	return &BookingStore{
		bookings: make(map[string]interface{}),
		places:   make(map[string]string),
	}
}

func (s *BookingStore) Book(placeID, carNumber string, hours int) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, occupied := s.places[placeID]; occupied {
		return "", false
	}
	bookingID := uuid.New().String()
	s.bookings[bookingID] = struct {
		PlaceID   string
		CarNumber string
		Hours     int
		Time      time.Time
	}{placeID, carNumber, hours, time.Now()}
	s.places[placeID] = bookingID
	return bookingID, true
}

func main() {
	logger := log.New(os.Stdout, "[BOOKING-AGENT] ", log.LstdFlags|log.Lmsgprefix)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Fatalf("Ошибка: %v", err)
	}
	defer nc.Drain()

	logger.Printf("Подключён к NATS: %s", natsURL)

	store := NewBookingStore()

	sub, err := nc.Subscribe("parking.book", func(msg *nats.Msg) {
		logger.Printf("Получено: %s", string(msg.Data))

		var envelope TaskEnvelope
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			logger.Printf("Ошибка парсинга конверта: %v", err)
			return
		}

		var req BookingRequest
		if err := json.Unmarshal([]byte(envelope.Payload), &req); err != nil {
			logger.Printf("Ошибка парсинга payload: %v", err)
			return
		}

		bookingID, success := store.Book(req.PlaceID, req.CarNumber, req.Hours)

		var result BookingResult
		if success {
			result = BookingResult{
				BookingID: bookingID,
				Success:   true,
				Message:   "Место забронировано",
			}
			logger.Printf("Бронирование успешно: %s", bookingID)
		} else {
			result = BookingResult{
				Success: false,
				Message: "Место уже занято",
			}
			logger.Printf("Бронирование отклонено: место %s занято", req.PlaceID)
		}

		event := CompletedEvent{
			TaskID:  envelope.ID,
			Agent:   "booking",
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

	logger.Println("Агент бронирования запущен")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Println("Завершение")
}