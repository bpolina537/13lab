import asyncio
import json
import uuid
import logging
from typing import Dict, Optional

import nats
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

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
        await self.nc.subscribe("llm.result", cb=self._on_result)
        logger.info("Подписка на tasks.completed и llm.result")

    async def _on_result(self, msg):
        data = json.loads(msg.data.decode())
        logger.info(f"Получен ответ: {data}")

        # Для LLM-агента
        if "task_id" in data and "result" in data:
            task_id = data["task_id"]
            if task_id in self.results:
                self.results[task_id].set_result(data)
                return

        # Для обычных агентов
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

    async def send_task_auction(self, subject: str, payload: dict, timeout: int = 10) -> dict:
        """Отправляет задачу через аукцион, выбирает лучшего агента"""
        task_id = str(uuid.uuid4())
        task = {"id": task_id, "type": subject, "payload": json.dumps(payload)}

        bids = {}

        async def on_bid(msg):
            data = json.loads(msg.data.decode())
            if data.get("task_id") == task_id:
                bids[data["agent_id"]] = {
                    "bid": data["bid"],
                    "agent_zone": data.get("agent_zone", "?"),
                    "load": data.get("load", 0)
                }
                logger.info(f"Ставка: агент={data['agent_id']}, цена={data['bid']}")

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

        logger.info(f"🏆 Победитель: {winner_id} (цена={winner['bid']})")

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

    async def run_pipeline(self, zone: str, car_number: str, hours: int, use_auction: bool = True) -> dict:
        """Запускает полный pipeline: поиск → бронь → доступ → оплата"""
        try:
            # Шаг 1: Поиск (с аукционом или без)
            if use_auction:
                logger.info(f"Поиск в зоне {zone} (аукцион)")
                search_result = await self.send_task_auction("parking.search", {"zone": zone})
            else:
                logger.info(f"Поиск в зоне {zone}")
                search_result = await self.send_task("parking.search", {"zone": zone})

            places = search_result.get("result", {}).get("places", [])
            if not places:
                raise Exception("Нет свободных мест")
            place_id = places[0]

            # Шаг 2: Бронирование
            logger.info(f"Бронирование {place_id}")
            book_result = await self.send_task("parking.book", {
                "place_id": place_id, "car_number": car_number, "hours": hours
            })
            booking_id = book_result.get("result", {}).get("booking_id")
            if not booking_id:
                raise Exception("Не удалось получить booking_id")

            # Шаг 3: Въезд
            logger.info(f"Въезд {car_number}")
            await self.send_task("parking.access", {
                "booking_id": booking_id, "car_number": car_number, "action": "enter"
            })

            # Шаг 4: Оплата
            logger.info(f"Оплата {hours} часов")
            payment_result = await self.send_task("parking.payment", {
                "booking_id": booking_id, "hours": hours
            })
            amount = payment_result.get("result", {}).get("amount", 0)

            return {
                "status": "success",
                "place_id": place_id,
                "booking_id": booking_id,
                "amount": amount,
                "currency": "RUB"
            }
        except Exception as e:
            logger.error(f"Pipeline ошибка: {e}")
            return {"status": "failed", "error": str(e)}


app = FastAPI(title="Parking System API")
orchestrator = PipelineOrchestrator()


class PipelineRequest(BaseModel):
    zone: str
    car_number: str
    hours: int


class LLMPipelineRequest(BaseModel):
    text: str


@app.on_event("startup")
async def startup():
    await orchestrator.connect()


@app.post("/pipeline")
async def run_pipeline(request: PipelineRequest):
    result = await orchestrator.run_pipeline(
        zone=request.zone,
        car_number=request.car_number,
        hours=request.hours,
        use_auction=True
    )
    if result["status"] == "failed":
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


@app.post("/pipeline/no_auction")
async def run_pipeline_no_auction(request: PipelineRequest):
    result = await orchestrator.run_pipeline(
        zone=request.zone,
        car_number=request.car_number,
        hours=request.hours,
        use_auction=False
    )
    if result["status"] == "failed":
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


@app.post("/llm_pipeline")
async def llm_pipeline(request: LLMPipelineRequest):
    """Принимает текст, отправляет в LLM, затем запускает pipeline"""
    task_id = str(uuid.uuid4())

    logger.info(f"LLM запрос: {request.text}")

    # Отправляем запрос в LLM-агента
    future = asyncio.Future()
    orchestrator.results[task_id] = future

    await orchestrator.nc.publish("llm.parse", json.dumps({
        "task_id": task_id,
        "text": request.text
    }).encode())

    try:
        result = await asyncio.wait_for(future, timeout=15)
        params = result.get("result", {})

        zone = params.get("zone", "A")
        hours = params.get("hours", 2)

        logger.info(f"LLM распознал: зона={zone}, часов={hours}")

        # Запускаем pipeline с полученными параметрами
        pipeline_result = await orchestrator.run_pipeline(
            zone=zone,
            car_number="LLM_AUTO",
            hours=hours,
            use_auction=True
        )
        return pipeline_result
    except asyncio.TimeoutError:
        logger.error("LLM агент не ответил")
        raise HTTPException(status_code=400, detail="LLM агент не ответил")


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)