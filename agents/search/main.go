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

var zoneMap = map[string][]string{
	"A": {"A1", "A2", "A3"},
	"B": {"B1", "B2"},
	"C": {"C1", "C2", "C3", "C4"},
}

var tracer trace.Tracer

func initTracer() func() {
	exporter, _ := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint("jaeger:4317"),
		otlptracegrpc.WithInsecure(),
	)

	res, _ := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("search-agent"),
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("search-agent")
	return func() { _ = tp.Shutdown(context.Background()) }
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	logger := log.New(os.Stdout, "[SEARCH-AGENT] ", log.LstdFlags|log.Lmsgprefix)

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

	sub, err := nc.Subscribe("parking.search", func(msg *nats.Msg) {
		// Извлекаем trace context из NATS заголовков (если есть)
		ctx := context.Background()

		var envelope TaskEnvelope
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			logger.Printf("Ошибка: %v", err)
			return
		}

		var req SearchRequest
		if err := json.Unmarshal([]byte(envelope.Payload), &req); err != nil {
			logger.Printf("Ошибка: %v", err)
			return
		}

		// Создаём span для обработки
		ctx, span := tracer.Start(ctx, "search.handle",
			trace.WithAttributes(attribute.String("zone", req.Zone)),
		)
		defer span.End()

		places, ok := zoneMap[req.Zone]
		if !ok {
			places = []string{}
		}

		result := SearchResult{Zone: req.Zone, Places: places}
		logger.Printf("Найдено: %v", places)

		event := CompletedEvent{
			TaskID:  envelope.ID,
			Agent:   "search",
			Subject: msg.Subject,
			Result:  result,
		}
		payload, _ := json.Marshal(event)
		nc.Publish("tasks.completed", payload)
		span.SetAttributes(attribute.Int("places_count", len(places)))
	})
	if err != nil {
		logger.Fatalf("Ошибка подписки: %v", err)
	}
	defer sub.Unsubscribe()

	logger.Println("Агент поиска запущен")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Println("Завершение")
}