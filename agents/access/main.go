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

// AccessRequest — входящий запрос на контроль доступа
type AccessRequest struct {
	BookingID string `json:"booking_id"`
	CarNumber string `json:"car_number"`
	Action    string `json:"action"` // "enter" или "exit"
}

// BookingInfo — минимальные данные брони, известные агенту доступа
type BookingInfo struct {
	CarNumber string
	IsInside  bool
}

// AccessResult — результат проверки доступа
type AccessResult struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	BookingID string `json:"booking_id"`
}

// CompletedEvent — публикуется в tasks.completed после обработки
type CompletedEvent struct {
	Agent   string      `json:"agent"`
	Subject string      `json:"subject"`
	Result  interface{} `json:"result"`
}

// AccessStore — потокобезопасное хранилище данных доступа
// В реальной системе агент получал бы данные от агента бронирования через NATS.
// Здесь — встроенный реестр с методом регистрации брони.
type AccessStore struct {
	mu       sync.RWMutex
	bookings map[string]*BookingInfo // booking_id -> info
}

func NewAccessStore() *AccessStore {
	return &AccessStore{
		bookings: make(map[string]*BookingInfo),
	}
}

// Register — зарегистрировать бронь (вызывается при получении события от агента бронирования)
func (s *AccessStore) Register(bookingID, carNumber string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bookings[bookingID] = &BookingInfo{
		CarNumber: carNumber,
		IsInside:  false,
	}
}

// CheckAccess — проверить доступ и обновить статус
func (s *AccessStore) CheckAccess(bookingID, carNumber, action string) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, ok := s.bookings[bookingID]
	if !ok {
		return false, "Бронирование не найдено"
	}
	if info.CarNumber != carNumber {
		return false, "Номер автомобиля не совпадает с бронированием"
	}

	switch action {
	case "enter":
		if info.IsInside {
			return false, "Автомобиль уже находится на парковке"
		}
		info.IsInside = true
		return true, "Шлагбаум открыт — въезд разрешён"
	case "exit":
		if !info.IsInside {
			return false, "Автомобиль не зарегистрирован как въехавший"
		}
		info.IsInside = false
		return true, "Шлагбаум открыт — выезд разрешён"
	default:
		return false, "Неизвестное действие: допустимы 'enter' или 'exit'"
	}
}

func main() {
	logger := log.New(os.Stdout, "[ACCESS-AGENT] ", log.LstdFlags|log.Lmsgprefix)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL,
		nats.Name("parking-access-agent"),
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

	store := NewAccessStore()

	// Слушаем tasks.completed чтобы получать подтверждённые брони от агента бронирования
	_, err = nc.Subscribe("tasks.completed", func(msg *nats.Msg) {
		var event struct {
			Agent  string `json:"agent"`
			Result struct {
				BookingID string `json:"booking_id"`
				Success   bool   `json:"success"`
				// CarNumber недоступен напрямую в результате — нужно хранить при запросе
				// В production: использовать request-reply или общее хранилище
			} `json:"result"`
		}
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			return
		}
		// Регистрируем только успешные брони от агента бронирования
		// CarNumber будет получен из запроса на доступ при первом обращении
		if event.Agent == "booking" && event.Result.Success && event.Result.BookingID != "" {
			logger.Printf("Получено новое бронирование: %s — ожидаем запрос на доступ", event.Result.BookingID)
		}
	})
	if err != nil {
		logger.Fatalf("Ошибка подписки на tasks.completed: %v", err)
	}

	// Основная подписка на канал доступа
	sub, err := nc.Subscribe("parking.access", func(msg *nats.Msg) {
		logger.Printf("Получено сообщение на канале %s: %s", msg.Subject, string(msg.Data))

		var req AccessRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			logger.Printf("Ошибка парсинга запроса: %v", err)
			return
		}

		if req.BookingID == "" || req.CarNumber == "" || req.Action == "" {
			logger.Printf("Ошибка: невалидные данные запроса: %+v", req)
			return
		}

		// Авторегистрация: если booking_id ещё не известен агенту —
		// регистрируем при первом обращении (упрощение без общего хранилища)
		store.mu.Lock()
		if _, exists := store.bookings[req.BookingID]; !exists {
			store.bookings[req.BookingID] = &BookingInfo{
				CarNumber: req.CarNumber,
				IsInside:  false,
			}
			logger.Printf("Автоматически зарегистрирована бронь %s для авто %s", req.BookingID, req.CarNumber)
		}
		store.mu.Unlock()

		success, message := store.CheckAccess(req.BookingID, req.CarNumber, req.Action)

		result := AccessResult{
			Success:   success,
			Message:   message,
			Action:    req.Action,
			BookingID: req.BookingID,
		}

		if success {
			logger.Printf("Доступ разрешён: booking_id=%s, авто=%s, действие=%s",
				req.BookingID, req.CarNumber, req.Action)
		} else {
			logger.Printf("Доступ отклонён: booking_id=%s, авто=%s, действие=%s, причина=%s",
				req.BookingID, req.CarNumber, req.Action, message)
		}

		event := CompletedEvent{
			Agent:   "access",
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
		logger.Fatalf("Ошибка подписки на parking.access: %v", err)
	}
	defer sub.Unsubscribe()

	logger.Println("Агент запущен, ожидание сообщений на канале parking.access...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Println("Завершение работы агента контроля доступа")
}
