import asyncio
import json
import aiohttp
import nats

NATS_URL = "nats://localhost:4222"
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:1b"

PROMPT_TEMPLATE = """
Ты помощник системы управления парковкой. Из запроса пользователя извлеки параметры для бронирования.

Запрос: {user_text}

Ответь ТОЛЬКО JSON в формате:
{{"zone": "A", "preference": "elevator", "hours": 2}}

Правила:
- zone: A, B или C. Если не указана, поставь A
- preference: elevator, exit или standard. Если не указано, standard
- hours: число от 1 до 8. Если не указано, 2

Примеры:
"хочу место у лифта в зоне С на 3 часа" -> {{"zone": "C", "preference": "elevator", "hours": 3}}
"нужно место у выхода на 2 часа" -> {{"zone": "A", "preference": "exit", "hours": 2}}
"зона B" -> {{"zone": "B", "preference": "standard", "hours": 2}}

Только JSON, без пояснений.
"""


async def parse_with_llm(user_text: str) -> dict:
    prompt = PROMPT_TEMPLATE.format(user_text=user_text)

    async with aiohttp.ClientSession() as session:
        async with session.post(OLLAMA_URL, json={
            "model": MODEL,
            "prompt": prompt,
            "stream": False,
            "temperature": 0.1
        }) as resp:
            data = await resp.json()
            response = data.get("response", "{}")
            print(f"[LLM-AGENT] Raw response: {response}")

            try:
                start = response.find('{')
                end = response.rfind('}') + 1
                if start != -1 and end != 0:
                    json_str = response[start:end]
                    return json.loads(json_str)
            except Exception as e:
                print(f"[LLM-AGENT] Parse error: {e}")

            return {"zone": "A", "preference": "standard", "hours": 2}


async def main():
    print("[LLM-AGENT] Starting...")
    nc = await nats.connect(NATS_URL)
    print(f"[LLM-AGENT] Connected to NATS")

    async def handler(msg):
        data = json.loads(msg.data.decode())
        user_text = data.get("text", "")
        task_id = data.get("task_id", "")
        print(f"[LLM-AGENT] Request: {user_text}")
        result = await parse_with_llm(user_text)
        print(f"[LLM-AGENT] Result: {result}")
        await nc.publish("llm.result", json.dumps({
            "task_id": task_id,
            "result": result
        }).encode())

    await nc.subscribe("llm.parse", cb=handler)
    print("[LLM-AGENT] Waiting for messages...")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())