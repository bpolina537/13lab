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

// BookingRequest — входящий запрос на бронирование
type BookingRequest struct {
	PlaceID   string `json:"place_id"`
	CarNumber string `json:"car_number"`
	Hours     int    `json:"hours"`
}

// BookingRecord — запись о бронировании в хранилище
type BookingRecord struct {
	BookingID string    `json:"booking_id"`
	PlaceID   string    `json:"place_id"`
	CarNumber string    `json:"car_number"`
	Hours     int       `json:"hours"`
	CreatedAt time.Time `json:"created_at"`
}

// BookingResult — результат бронирования
type BookingResult struct {
	BookingID string `json:"booking_id"`
	Success   bool   `json:"success"`
	Message   string `json:"message,omitempty"`
}

// CompletedEvent — публикуется в tasks.completed после обработки
type CompletedEvent struct {
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

// BookingStore — потокобезопасное хранилище броней
type BookingStore struct {
	mu       sync.RWMutex
	bookings map[string]*BookingRecord // booking_id -> запись
	places   map[string]string         // place_id -> booking_id (занятые места)
}

func NewBookingStore() *BookingStore {
	return &BookingStore{
		bookings: make(map[string]*BookingRecord),
		places:   make(map[string]string),
	}
}

// Book — атомарно проверяет и бронирует место
func (s *BookingStore) Book(placeID, carNumber string, hours int) (string, error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, occupied := s.places[placeID]; occupied {
		return "", nil, false
	}

	bookingID := uuid.New().String()
	record := &BookingRecord{
		BookingID: bookingID,
		PlaceID:   placeID,
		CarNumber: carNumber,
		Hours:     hours,
		CreatedAt: time.Now(),
	}
	s.bookings[bookingID] = record
	s.places[placeID] = bookingID

	return bookingID, nil, true
}

// Get — получить запись по ID брони (для других агентов, если нужно расширить)
func (s *BookingStore) Get(bookingID string) (*BookingRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.bookings[bookingID]
	return rec, ok
}

func main() {
	logger := log.New(os.Stdout, "[BOOKING-AGENT] ", log.LstdFlags|log.Lmsgprefix)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL,
		nats.Name("parking-booking-agent"),
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

	store := NewBookingStore()

	sub, err := nc.Subscribe("parking.book", func(msg *nats.Msg) {
		logger.Printf("Получено сообщение на канале %s: %s", msg.Subject, string(msg.Data))

		var req BookingRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			logger.Printf("Ошибка парсинга запроса: %v", err)
			return
		}

		if req.PlaceID == "" || req.CarNumber == "" || req.Hours <= 0 {
			logger.Printf("Ошибка: невалидные данные запроса: %+v", req)
			return
		}

		bookingID, _, success := store.Book(req.PlaceID, req.CarNumber, req.Hours)

		var result BookingResult
		if success {
			result = BookingResult{
				BookingID: bookingID,
				Success:   true,
				Message:   "Место успешно забронировано",
			}
			logger.Printf("Бронирование успешно: booking_id=%s, место=%s, авто=%s, часов=%d",
				bookingID, req.PlaceID, req.CarNumber, req.Hours)
		} else {
			result = BookingResult{
				BookingID: "",
				Success:   false,
				Message:   "Место уже занято",
			}
			logger.Printf("Бронирование отклонено: место %s уже занято", req.PlaceID)
		}

		event := CompletedEvent{
			Agent:   "booking",
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
		logger.Fatalf("Ошибка подписки на parking.book: %v", err)
	}
	defer sub.Unsubscribe()

	logger.Println("Агент запущен, ожидание сообщений на канале parking.book...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Println("Завершение работы агента бронирования")
}
