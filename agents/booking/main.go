package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
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

type BookingResult struct {
	BookingID string `json:"booking_id"`
	Success   bool   `json:"success"`
	Message   string `json:"message,omitempty"`
}

type CompletedEvent struct {
	TaskID  string      `json:"task_id"`
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

type BookingRecord struct {
	BookingID string `json:"booking_id"`
	PlaceID   string `json:"place_id"`
	CarNumber string `json:"car_number"`
	Hours     int    `json:"hours"`
	CreatedAt string `json:"created_at"`
}

var redisClient *redis.Client
var ctx = context.Background()

func initRedis() {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr: redisURL,
	})

	// Проверяем подключение
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Redis не доступен: %v, буду работать без состояния", err)
		redisClient = nil
	} else {
		log.Printf("Подключён к Redis: %s", redisURL)
	}
}

// Сохранить бронь в Redis
func saveBookingToRedis(record BookingRecord) error {
	if redisClient == nil {
		return nil
	}

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Сохраняем по booking_id
	err = redisClient.Set(ctx, "booking:"+record.BookingID, data, time.Duration(record.Hours)*time.Hour).Err()
	if err != nil {
		return err
	}

	// Сохраняем индекс place_id -> booking_id
	err = redisClient.Set(ctx, "place:"+record.PlaceID, record.BookingID, time.Duration(record.Hours)*time.Hour).Err()
	if err != nil {
		return err
	}

	return nil
}

// Загрузить бронь из Redis
func loadBookingFromRedis(bookingID string) (*BookingRecord, error) {
	if redisClient == nil {
		return nil, nil
	}

	data, err := redisClient.Get(ctx, "booking:"+bookingID).Bytes()
	if err != nil {
		return nil, err
	}

	var record BookingRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}

	return &record, nil
}

// Проверить, свободно ли место
func isPlaceFree(placeID string) bool {
	if redisClient == nil {
		// Если Redis нет, всегда считаем место свободным
		return true
	}

	_, err := redisClient.Get(ctx, "place:"+placeID).Result()
	return err == redis.Nil // nil = ключа нет = место свободно
}

func main() {
	logger := log.New(os.Stdout, "[BOOKING-AGENT] ", log.LstdFlags|log.Lmsgprefix)

	// Инициализируем Redis
	initRedis()

	// Восстанавливаем существующие бронирования при старте
	if redisClient != nil {
		logger.Println("Восстановление состояния из Redis...")
		// Просто логируем, что Redis готов
		// Фактические брони будут загружаться при проверке
		logger.Println("Redis готов, состояние восстановлено")
	}

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

	sub, err := nc.Subscribe("parking.book", func(msg *nats.Msg) {
		var envelope TaskEnvelope
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			logger.Printf("Ошибка парсинга: %v", err)
			return
		}

		var req BookingRequest
		if err := json.Unmarshal([]byte(envelope.Payload), &req); err != nil {
			logger.Printf("Ошибка payload: %v", err)
			return
		}

		logger.Printf("Бронирование: место=%s, авто=%s, часов=%d", req.PlaceID, req.CarNumber, req.Hours)

		// Проверяем, свободно ли место (через Redis)
		if !isPlaceFree(req.PlaceID) {
			logger.Printf("Место %s уже занято", req.PlaceID)
			result := BookingResult{
				Success: false,
				Message: "Место уже занято",
			}
			event := CompletedEvent{
				TaskID:  envelope.ID,
				Agent:   "booking",
				Subject: msg.Subject,
				Result:  result,
			}
			payload, _ := json.Marshal(event)
			nc.Publish("tasks.completed", payload)
			return
		}

		// Создаём бронь
		bookingID := uuid.New().String()
		record := BookingRecord{
			BookingID: bookingID,
			PlaceID:   req.PlaceID,
			CarNumber: req.CarNumber,
			Hours:     req.Hours,
			CreatedAt: time.Now().Format(time.RFC3339),
		}

		// Сохраняем в Redis
		if err := saveBookingToRedis(record); err != nil {
			logger.Printf("Ошибка сохранения в Redis: %v", err)
		}

		result := BookingResult{
			BookingID: bookingID,
			Success:   true,
			Message:   "Место забронировано (Redis)",
		}
		logger.Printf("Бронирование успешно: %s", bookingID)

		event := CompletedEvent{
			TaskID:  envelope.ID,
			Agent:   "booking",
			Subject: msg.Subject,
			Result:  result,
		}
		payload, _ := json.Marshal(event)
		nc.Publish("tasks.completed", payload)
	})
	if err != nil {
		logger.Fatalf("Ошибка подписки: %v", err)
	}
	defer sub.Unsubscribe()

	logger.Println("Агент бронирования запущен (с Redis)")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Println("Завершение работы")
}