package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
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

type PaymentRequest struct {
	BookingID string `json:"booking_id"`
	Hours     int    `json:"hours"`
}

type PaymentResult struct {
	BookingID string  `json:"booking_id"`
	Hours     int     `json:"hours"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
	Discount  bool    `json:"discount"`
}

type CompletedEvent struct {
	TaskID  string      `json:"task_id"`
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

const (
	basePrice  = 100.0
	discountTh = 5
	discount   = 0.2
)

func calculateAmount(hours int) (float64, bool) {
	total := float64(hours) * basePrice
	if hours > discountTh {
		return total * (1 - discount), true
	}
	return total, false
}

var tracer trace.Tracer

func initTracer() func() {
	exporter, _ := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint("jaeger:4317"),
		otlptracegrpc.WithInsecure(),
	)

	res, _ := resource.New(context.Background(),
		resource.WithAttributes(semconv.ServiceNameKey.String("payment-agent")),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("payment-agent")
	return func() { _ = tp.Shutdown(context.Background()) }
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	logger := log.New(os.Stdout, "[PAYMENT-AGENT] ", log.LstdFlags|log.Lmsgprefix)

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

	sub, err := nc.Subscribe("parking.payment", func(msg *nats.Msg) {
		ctx := context.Background()

		var envelope TaskEnvelope
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			logger.Printf("Ошибка парсинга: %v", err)
			return
		}

		var req PaymentRequest
		if err := json.Unmarshal([]byte(envelope.Payload), &req); err != nil {
			logger.Printf("Ошибка payload: %v", err)
			return
		}

		ctx, span := tracer.Start(ctx, "payment.calculate",
			trace.WithAttributes(
				attribute.String("task.id", envelope.ID),
				attribute.String("booking_id", req.BookingID),
				attribute.Int("hours", req.Hours),
			),
		)
		defer span.End()

		amount, hasDiscount := calculateAmount(req.Hours)

		result := PaymentResult{
			BookingID: req.BookingID,
			Hours:     req.Hours,
			Amount:    amount,
			Currency:  "RUB",
			Discount:  hasDiscount,
		}

		logger.Printf("Сумма: %.2f RUB (скидка=%v)", amount, hasDiscount)
		span.SetAttributes(attribute.Float64("amount", amount))

		event := CompletedEvent{
			TaskID:  envelope.ID,
			Agent:   "payment",
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

	logger.Println("Агент оплаты запущен (с трассировкой)")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Println("Завершение")
}