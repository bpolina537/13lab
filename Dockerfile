FROM golang:1.21-alpine

WORKDIR /app

# Устанавливаем зависимости для go run (нужен git для go mod download)
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

# Копируем весь исходный код
COPY agents/ ./agents/

# Команда запуска передаётся через command: в docker-compose.yml
# Например: ["go", "run", "./agents/search"]
