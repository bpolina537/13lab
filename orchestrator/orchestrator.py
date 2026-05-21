import asyncio
import json
import uuid
import logging
from typing import Dict, Optional

import nats
from nats.aio.client import Client as NATSClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    def __init__(self):
        self.nc: Optional[NATSClient] = None
        self.results: Dict[str, asyncio.Future] = {}

    async def connect(self):
        self.nc = await nats.connect("nats://localhost:4222")
        logger.info("Оркестратор подключён к NATS")

        # Подписка на результаты
        await self.nc.subscribe("tasks.completed", cb=self._on_result)
        logger.info("Подписка на tasks.completed")

    async def _on_result(self, msg):
        data = json.loads(msg.data.decode())
        task_id = None

        # Извлекаем task_id из ответа
        if "task_id" in data:
            task_id = data["task_id"]
        elif "result" in data and "task_id" in data["result"]:
            task_id = data["result"]["task_id"]
        elif "result" in data and "booking_id" in data["result"]:
            # Ищем по booking_id в Future?
            for tid, future in list(self.results.items()):
                if not future.done():
                    task_id = tid
                    break

        if task_id and task_id in self.results:
            self.results[task_id].set_result(data)
            logger.debug(f"Результат для {task_id} получен")

    async def send_task(self, subject: str, payload: dict, timeout: int = 10, max_retries: int = 3) -> dict:
        """Отправляет задачу и ждёт ответа"""
        task_id = str(uuid.uuid4())
        task = {
            "id": task_id,
            "type": subject,
            "payload": json.dumps(payload)
        }

        for attempt in range(max_retries):
            future = asyncio.Future()
            self.results[task_id] = future

            try:
                logger.info(f"Отправка задачи {subject} (попытка {attempt + 1})")
                await self.nc.publish(subject, json.dumps(task).encode())

                result = await asyncio.wait_for(future, timeout=timeout)
                logger.info(f"Задача {subject} выполнена")

                # Проверяем success
                if "result" in result:
                    if result["result"].get("success") is False:
                        raise Exception(f"Ошибка: {result['result'].get('message', 'неизвестная ошибка')}")

                return result

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут {subject}, попытка {attempt + 1}")
                if attempt == max_retries - 1:
                    raise TimeoutError(f"Задача {subject} не выполнена за {timeout} сек после {max_retries} попыток")
                await asyncio.sleep(2 ** attempt)  # exponential backoff
            except Exception as e:
                logger.error(f"Ошибка {subject}: {e}")
                if attempt == max_retries - 1:
                    raise
            finally:
                if task_id in self.results:
                    del self.results[task_id]

        raise RuntimeError("Неожиданная ошибка")

    async def run_pipeline(self, zone: str, car_number: str, hours: int) -> dict:
        """Запускает полный pipeline: поиск → бронь → доступ → оплата"""
        try:
            # Шаг 1: Поиск мест
            logger.info(f"Pipeline: поиск мест в зоне {zone}")
            search_result = await self.send_task("parking.search", {"zone": zone})

            # Извлекаем список мест
            places = []
            if "result" in search_result:
                places = search_result["result"].get("places", [])
            elif "places" in search_result:
                places = search_result.get("places", [])

            if not places:
                raise Exception("Нет свободных мест в данной зоне")

            place_id = places[0]
            logger.info(f"Pipeline: выбрано место {place_id}")

            # Шаг 2: Бронирование
            logger.info(f"Pipeline: бронирование места {place_id}")
            book_result = await self.send_task("parking.book", {
                "place_id": place_id,
                "car_number": car_number,
                "hours": hours
            })

            booking_id = None
            if "result" in book_result:
                booking_id = book_result["result"].get("booking_id")
            elif "booking_id" in book_result:
                booking_id = book_result.get("booking_id")

            if not booking_id:
                raise Exception("Не удалось получить booking_id")

            logger.info(f"Pipeline: получен booking_id {booking_id}")

            # Шаг 3: Въезд
            logger.info(f"Pipeline: въезд для {car_number}")
            await self.send_task("parking.access", {
                "booking_id": booking_id,
                "car_number": car_number,
                "action": "enter"
            })

            # Шаг 4: Оплата
            logger.info(f"Pipeline: расчёт оплаты за {hours} часов")
            payment_result = await self.send_task("parking.payment", {
                "booking_id": booking_id,
                "hours": hours
            })

            amount = 0
            if "result" in payment_result:
                amount = payment_result["result"].get("amount", 0)
            elif "amount" in payment_result:
                amount = payment_result.get("amount", 0)

            logger.info(f"Pipeline: успешно завершён. Сумма: {amount} RUB")

            return {
                "status": "success",
                "place_id": place_id,
                "booking_id": booking_id,
                "amount": amount,
                "currency": "RUB"
            }

        except Exception as e:
            logger.error(f"Pipeline ошибка: {e}")
            return {
                "status": "failed",
                "error": str(e)
            }