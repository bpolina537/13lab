package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/nats-io/nats.go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
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
		s.bookings[bookingID] = struct {
			CarNumber string
			IsInside  bool
		}{carNumber, false}
		info = s.bookings[bookingID]
	}

	if info.CarNumber != carNumber {
		return false, "Номер не совпадает"
	}

	switch action {
	case "enter":
		if info.IsInside {
			return false, "Уже на парковке"
		}
		info.IsInside = true
		s.bookings[bookingID] = info
		return true, "Въезд разрешён"
	case "exit":
		if !info.IsInside {
			return false, "Не на парковке"
		}
		info.IsInside = false
		s.bookings[bookingID] = info
		return true, "Выезд разрешён"
	default:
		return false, "Неизвестное действие"
	}
}

var tracer trace.Tracer

func initTracer() func() {
	exporter, _ := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint("jaeger:4317"),
		otlptracegrpc.WithInsecure(),
	)

	res, _ := resource.New(context.Background(),
		resource.WithAttributes(semconv.ServiceNameKey.String("access-agent")),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("access-agent")
	return func() { _ = tp.Shutdown(context.Background()) }
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	logger := log.New(os.Stdout, "[ACCESS-AGENT] ", log.LstdFlags|log.Lmsgprefix)

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

	store := NewAccessStore()

	sub, err := nc.Subscribe("parking.access", func(msg *nats.Msg) {
		ctx := context.Background()

		var envelope TaskEnvelope
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			logger.Printf("Ошибка парсинга: %v", err)
			return
		}

		var req AccessRequest
		if err := json.Unmarshal([]byte(envelope.Payload), &req); err != nil {
			logger.Printf("Ошибка payload: %v", err)
			return
		}

		ctx, span := tracer.Start(ctx, "access.check",
			trace.WithAttributes(
				attribute.String("task.id", envelope.ID),
				attribute.String("booking_id", req.BookingID),
				attribute.String("car_number", req.CarNumber),
				attribute.String("action", req.Action),
			),
		)
		defer span.End()

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
			span.SetAttributes(attribute.String("error", message))
		}

		event := CompletedEvent{
			TaskID:  envelope.ID,
			Agent:   "access",
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

	logger.Println("Агент доступа запущен (с трассировкой)")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Println("Завершение")
}