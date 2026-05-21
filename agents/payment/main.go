package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
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
	BookingID  string  `json:"booking_id"`
	Hours      int     `json:"hours"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
	Discount   bool    `json:"discount"`
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

func main() {
	logger := log.New(os.Stdout, "[PAYMENT-AGENT] ", log.LstdFlags|log.Lmsgprefix)

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

	sub, err := nc.Subscribe("parking.payment", func(msg *nats.Msg) {
		logger.Printf("Получено сообщение: %s", string(msg.Data))

		// 1. Парсим конверт от оркестратора
		var envelope TaskEnvelope
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			logger.Printf("Ошибка парсинга конверта: %v", err)
			return
		}

		// 2. Парсим payload (реальный запрос)
		var req PaymentRequest
		if err := json.Unmarshal([]byte(envelope.Payload), &req); err != nil {
			logger.Printf("Ошибка парсинга payload: %v", err)
			return
		}

		logger.Printf("Запрос оплаты: booking_id=%s, hours=%d", req.BookingID, req.Hours)

		amount, hasDiscount := calculateAmount(req.Hours)

		result := PaymentResult{
			BookingID: req.BookingID,
			Hours:     req.Hours,
			Amount:    amount,
			Currency:  "RUB",
			Discount:  hasDiscount,
		}

		if hasDiscount {
			logger.Printf("✅ Сумма со скидкой: %.2f RUB (часов=%d)", amount, req.Hours)
		} else {
			logger.Printf("✅ Сумма без скидки: %.2f RUB (часов=%d)", amount, req.Hours)
		}

		// 3. Отправляем ответ с task_id
		event := CompletedEvent{
			TaskID:  envelope.ID,
			Agent:   "payment",
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

	logger.Println("Агент оплаты запущен, ожидание сообщений...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Println("Завершение работы")
}