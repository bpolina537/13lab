import asyncio
import json
import uuid
import logging
from typing import Dict, Optional

import nats
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
        logger.info(f"Оркестратор получил ответ: {data}")

        # Ищем task_id в разных местах
        task_id = None

        # Вариант 1: в корне ответа
        if "task_id" in data:
            task_id = data["task_id"]
        # Вариант 2: в поле result (как у тебя в агентах)
        elif "result" in data and "task_id" in data["result"]:
            task_id = data["result"]["task_id"]
        # Вариант 3: нет task_id — нужно сопоставить по booking_id или другому полю
        else:
            # Логируем, что пришло, и ищем любой известный ID
            logger.warning(f"Ответ не содержит task_id: {data.keys()}")
            # Пытаемся найти Future по любому известному идентификатору
            for tid, future in list(self.results.items()):
                if not future.done():
                    task_id = tid
                    break

        if task_id and task_id in self.results:
            self.results[task_id].set_result(data)
            logger.info(f"Результат для {task_id} доставлен")
        else:
            logger.error(f"Не найден Future для task_id={task_id}. Доступные Future: {list(self.results.keys())}")

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
                logger.warning(f"Таймаут {subject}, попытка {attempt + 1}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Ошибка {subject}: {e}")
                if attempt == max_retries - 1:
                    raise
            finally:
                self.results.pop(task_id, None)

        raise RuntimeError("Неожиданная ошибка")

    async def run_pipeline(self, zone: str, car_number: str, hours: int) -> dict:
        try:
            logger.info(f"Поиск мест в зоне {zone}")
            search_result = await self.send_task("parking.search", {"zone": zone})
            places = search_result.get("result", {}).get("places", [])
            if not places:
                raise Exception("Нет свободных мест")
            place_id = places[0]

            logger.info(f"Бронирование {place_id}")
            book_result = await self.send_task("parking.book", {
                "place_id": place_id, "car_number": car_number, "hours": hours
            })
            booking_id = book_result.get("result", {}).get("booking_id")
            if not booking_id:
                raise Exception("Не удалось получить booking_id")

            logger.info(f"Въезд {car_number}")
            await self.send_task("parking.access", {
                "booking_id": booking_id, "car_number": car_number, "action": "enter"
            })

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


@app.on_event("startup")
async def startup():
    await orchestrator.connect()


@app.post("/pipeline")
async def run_pipeline(request: PipelineRequest):
    result = await orchestrator.run_pipeline(
        zone=request.zone,
        car_number=request.car_number,
        hours=request.hours
    )
    if result["status"] == "failed":
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)