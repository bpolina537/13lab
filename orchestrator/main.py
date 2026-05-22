import asyncio
import json
import uuid
import logging
from typing import Dict, Optional

import nats
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# Настройка трассировки
provider = TracerProvider()
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    def __init__(self):
        self.nc: Optional[nats.NATS] = None
        self.results: Dict[str, asyncio.Future] = {}

    async def connect(self):
        self.nc = await nats.connect("nats://localhost:4222")
        logger.info("Оркестратор подключён к NATS")
        await self.nc.subscribe("tasks.completed", cb=self._on_result)
        logger.info("Подписка на tasks.completed")

    async def _on_result(self, msg):
        data = json.loads(msg.data.decode())
        task_id = data.get("task_id") or data.get("id")
        if not task_id and "result" in data:
            task_id = data["result"].get("task_id")

        if task_id and task_id in self.results:
            self.results[task_id].set_result(data)
            logger.info(f"Ответ для {task_id}")

    async def send_task(self, subject: str, payload: dict, timeout: int = 10, max_retries: int = 3) -> dict:
        task_id = str(uuid.uuid4())
        task = {"id": task_id, "type": subject, "payload": json.dumps(payload)}

        for attempt in range(max_retries):
            future = asyncio.Future()
            self.results[task_id] = future

            try:
                # Создаём span для этого шага
                with tracer.start_as_current_span(f"send_{subject}") as span:
                    span.set_attribute("task_id", task_id)
                    span.set_attribute("subject", subject)
                    span.set_attribute("attempt", attempt + 1)

                    logger.info(f"Отправка {subject} (попытка {attempt + 1})")
                    await self.nc.publish(subject, json.dumps(task).encode())
                    result = await asyncio.wait_for(future, timeout=timeout)

                    if result.get("result", {}).get("success") is False:
                        raise Exception(result.get("result", {}).get("message", "Ошибка"))
                    return result
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут {subject}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                if attempt == max_retries - 1:
                    raise
            finally:
                self.results.pop(task_id, None)

    async def run_pipeline(self, zone: str, car_number: str, hours: int) -> dict:
        # Корневой span для всего pipeline
        with tracer.start_as_current_span("pipeline") as parent_span:
            parent_span.set_attribute("zone", zone)
            parent_span.set_attribute("car_number", car_number)
            parent_span.set_attribute("hours", hours)

            try:
                logger.info(f"Поиск в зоне {zone}")
                search_result = await self.send_task_auction("parking.search", {"zone": zone})
                places = search_result.get("result", {}).get("places", [])
                if not places:
                    raise Exception("Нет мест")
                place_id = places[0]

                logger.info(f"Бронирование {place_id}")
                book_result = await self.send_task("parking.book",
                                                   {"place_id": place_id, "car_number": car_number, "hours": hours})
                booking_id = book_result.get("result", {}).get("booking_id")
                if not booking_id:
                    raise Exception("Нет booking_id")

                logger.info(f"Въезд {car_number}")
                await self.send_task("parking.access",
                                     {"booking_id": booking_id, "car_number": car_number, "action": "enter"})

                logger.info(f"Оплата {hours} ч")
                payment_result = await self.send_task("parking.payment", {"booking_id": booking_id, "hours": hours})
                amount = payment_result.get("result", {}).get("amount", 0)

                return {"status": "success", "place_id": place_id, "booking_id": booking_id, "amount": amount,
                        "currency": "RUB"}
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                return {"status": "failed", "error": str(e)}

    async def send_task_auction(self, subject: str, payload: dict, timeout: int = 10) -> dict:
        """Отправляет задачу через аукцион, выбирает лучшего агента"""
        task_id = str(uuid.uuid4())
        task = {"id": task_id, "type": subject, "payload": json.dumps(payload)}

        bids = {}
        bid_future = asyncio.Future()

        async def on_bid(msg):
            data = json.loads(msg.data.decode())
            if data.get("task_id") == task_id:
                bids[data["agent_id"]] = {
                    "bid": data["bid"],
                    "agent_zone": data.get("agent_zone", "?"),
                    "load": data.get("load", 0)
                }
                logger.info(f"Ставка: агент={data['agent_id']}, цена={data['bid']}, нагрузка={data.get('load', 0)}")

        # Подписываемся на ставки
        await self.nc.subscribe("auction.search.bid", cb=on_bid)

        # Отправляем запрос на аукцион
        logger.info(f"📢 Аукцион для {subject}, task_id={task_id}")
        await self.nc.publish("auction.search.request", json.dumps({
            "task_id": task_id,
            "task": task
        }).encode())

        # Ждём ставки 3 секунды
        await asyncio.sleep(3)

        if not bids:
            raise Exception("Нет ставок на аукционе")

        # Выбираем победителя с минимальной ценой
        winner_id = min(bids.items(), key=lambda x: x[1]["bid"])[0]
        winner = bids[winner_id]

        logger.info(
            f"🏆 Победитель: {winner_id} (цена={winner['bid']}, зона={winner['agent_zone']}, нагрузка={winner['load']})")

        # Отправляем задачу победителю
        await self.nc.publish("auction.search.winner", json.dumps({
            "task_id": task_id,
            "agent_id": winner_id,
            "task": task
        }).encode())

        # Ждём результат
        future = asyncio.Future()
        self.results[task_id] = future
        return await asyncio.wait_for(future, timeout=timeout)


app = FastAPI(title="Parking System API")
orchestrator = PipelineOrchestrator()

# Автоматическая трассировка всех эндпоинтов FastAPI
FastAPIInstrumentor.instrument_app(app)


class PipelineRequest(BaseModel):
    zone: str
    car_number: str
    hours: int


@app.on_event("startup")
async def startup():
    await orchestrator.connect()


@app.post("/pipeline")
async def run_pipeline(request: PipelineRequest):
    with tracer.start_as_current_span("http_pipeline") as span:
        span.set_attribute("http.method", "POST")
        span.set_attribute("http.route", "/pipeline")
        result = await orchestrator.run_pipeline(request.zone, request.car_number, request.hours)
        if result["status"] == "failed":
            raise HTTPException(400, result["error"])
        return result


@app.get("/health")
async def health():
    return {"status": "ok"}



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)