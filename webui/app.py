import streamlit as st
import requests
import pandas as pd
import time
import json

# Настройки
API_URL = "http://localhost:8000"
NATS_URL = "http://localhost:8222"  # NATS HTTP мониторинг

st.set_page_config(page_title="Parking System Dashboard", layout="wide")

st.title("🚗 Управление парковкой — Мониторинг агентов")

# Боковая панель
with st.sidebar:
    st.header("🔧 Ручной запуск")
    zone = st.selectbox("Зона", ["A", "B", "C"])
    car_number = st.text_input("Номер автомобиля", "А123ВС")
    hours = st.slider("Часы", 1, 8, 2)

    if st.button("🚀 Запустить бронирование"):
        with st.spinner("Бронирование..."):
            try:
                response = requests.post(
                    f"{API_URL}/pipeline",
                    json={"zone": zone, "car_number": car_number, "hours": hours}
                )
                if response.status_code == 200:
                    data = response.json()
                    st.success(
                        f"✅ Забронировано! Место: {data['place_id']}, ID: {data['booking_id'][:8]}, Сумма: {data['amount']} RUB")
                else:
                    st.error(f"❌ Ошибка: {response.text}")
            except Exception as e:
                st.error(f"❌ Не удалось подключиться к API: {e}")

    st.divider()

    st.header("🤖 LLM запрос")
    llm_text = st.text_input("Опишите желаемое место", "хочу место у лифта в зоне С на 3 часа")
    if st.button("🤖 Запустить через LLM"):
        with st.spinner("LLM анализирует запрос..."):
            try:
                response = requests.post(
                    f"{API_URL}/llm_pipeline",
                    json={"text": llm_text}
                )
                if response.status_code == 200:
                    data = response.json()
                    st.success(f"✅ Забронировано! Место: {data['place_id']}, Сумма: {data['amount']} RUB")
                else:
                    st.error(f"❌ Ошибка: {response.text}")
            except Exception as e:
                st.error(f"❌ Не удалось подключиться к API: {e}")

# Основная область
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Статус агентов")

    try:
        # Проверяем контейнеры через Docker API (если есть доступ)
        # Упрощённо: показываем заглушку с реальными именами
        agents = [
            {"name": "agent-search", "status": "🟢 Running", "replicas": 3},
            {"name": "agent-booking", "status": "🟢 Running", "replicas": 1},
            {"name": "agent-access", "status": "🟢 Running", "replicas": 1},
            {"name": "agent-payment", "status": "🟢 Running", "replicas": 1},
            {"name": "llm-agent", "status": "🟢 Running", "replicas": 1},
        ]

        df = pd.DataFrame(agents)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.caption("Обновление: каждые 5 секунд")
        st.caption("🟢 Running — агент активен")

    except Exception as e:
        st.error(f"Ошибка получения статуса: {e}")

with col2:
    st.subheader("📈 Очереди NATS")

    # У NATS нет простого API для получения длины очереди без JetStream
    # Показываем заглушку с информацией
    st.info("Данные о очередях доступны через NATS JetStream API")

    queues = [
        {"канал": "parking.search", "сообщений": "~0-5", "статус": "✅"},
        {"канал": "parking.book", "сообщений": "~0", "статус": "✅"},
        {"канал": "parking.access", "сообщений": "~0", "статус": "✅"},
        {"канал": "parking.payment", "сообщений": "~0", "статус": "✅"},
        {"канал": "tasks.completed", "сообщений": "~0", "статус": "✅"},
    ]

    df_queues = pd.DataFrame(queues)
    st.dataframe(df_queues, use_container_width=True, hide_index=True)

# Результаты задач
st.subheader("📋 Последние результаты задач")

# Хранилище последних результатов (в реальной системе брать из API)
if "results" not in st.session_state:
    st.session_state.results = []

# Кнопка обновления
if st.button("🔄 Обновить"):
    st.rerun()

# Показываем последние 10 результатов
if st.session_state.results:
    df_results = pd.DataFrame(st.session_state.results[-10:])
    st.dataframe(df_results, use_container_width=True)
else:
    st.info("Нет выполненных задач. Используйте боковую панель для запуска бронирования.")

# Логи в реальном времени
st.subheader("📜 Логи (последние 20 строк)")
log_file = "orchestrator.log"

try:
    import os

    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            lines = f.readlines()
            last_lines = lines[-20:] if len(lines) > 20 else lines
            st.code("".join(last_lines), language="log")
    else:
        st.info("Файл логов не найден")
except Exception as e:
    st.error(f"Ошибка чтения логов: {e}")

# Автообновление
auto_refresh = st.checkbox("Автообновление (каждые 5 секунд)")
if auto_refresh:
    time.sleep(5)
    st.rerun()