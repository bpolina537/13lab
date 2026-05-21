package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
)

const (
	basePricePerHour  = 100.0 // рублей в час
	discountThreshold = 5     // часов — порог для скидки
	discountRate      = 0.20  // 20% скидка
)

// PaymentRequest — входящий запрос на расчёт оплаты
type PaymentRequest struct {
	BookingID string `json:"booking_id"`
	Hours     int    `json:"hours"`
}

// PaymentResult — результат расчёта
type PaymentResult struct {
	BookingID    string  `json:"booking_id"`
	Hours        int     `json:"hours"`
	Amount       float64 `json:"amount"`
	Currency     string  `json:"currency"`
	DiscountAppl bool    `json:"discount_applied"`
	DiscountPct  int     `json:"discount_percent,omitempty"`
}

// CompletedEvent — публикуется в tasks.completed после обработки
type CompletedEvent struct {
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

// calculateAmount — рассчитывает стоимость с учётом скидки
func calculateAmount(hours int) (amount float64, discountApplied bool) {
	total := basePricePerHour * float64(hours)
	if hours > discountThreshold {
		total = total * (1 - discountRate)
		return total, true
	}
	return total, false
}

func main() {
	logger := log.New(os.Stdout, "[PAYMENT-AGENT] ", log.LstdFlags|log.Lmsgprefix)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL,
		nats.Name("parking-payment-agent"),
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

	sub, err := nc.Subscribe("parking.payment", func(msg *nats.Msg) {
		logger.Printf("Получено сообщение на канале %s: %s", msg.Subject, string(msg.Data))

		var req PaymentRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			logger.Printf("Ошибка парсинга запроса: %v", err)
			return
		}

		if req.BookingID == "" {
			logger.Println("Ошибка: поле 'booking_id' не задано")
			return
		}
		if req.Hours <= 0 {
			logger.Printf("Ошибка: некорректное количество часов: %d", req.Hours)
			return
		}

		amount, discountApplied := calculateAmount(req.Hours)

		result := PaymentResult{
			BookingID:    req.BookingID,
			Hours:        req.Hours,
			Amount:       amount,
			Currency:     "RUB",
			DiscountAppl: discountApplied,
		}
		if discountApplied {
			result.DiscountPct = int(discountRate * 100)
			logger.Printf("Расчёт со скидкой: booking_id=%s, часов=%d, скидка=%d%%, сумма=%.2f RUB",
				req.BookingID, req.Hours, result.DiscountPct, amount)
		} else {
			logger.Printf("Расчёт без скидки: booking_id=%s, часов=%d, сумма=%.2f RUB",
				req.BookingID, req.Hours, amount)
		}

		event := CompletedEvent{
			Agent:   "payment",
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
		logger.Fatalf("Ошибка подписки на parking.payment: %v", err)
	}
	defer sub.Unsubscribe()

	logger.Println("Агент запущен, ожидание сообщений на канале parking.payment...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Println("Завершение работы агента расчёта оплаты")
}
