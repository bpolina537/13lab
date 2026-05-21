import asyncio
import subprocess
import nats
import os

NATS_URL = "nats://nats:4222"
QUEUE_NAME = "parking.search"
THRESHOLD = 5
CHECK_INTERVAL = 10

current_replicas = 1

def scale(replicas):
    global current_replicas
    if replicas == current_replicas:
        return
    print(f"Scaling agent-search to {replicas}")
    subprocess.run(["docker-compose", "up", "--scale", f"agent-search={replicas}", "-d"], check=True)
    current_replicas = replicas

async def main():
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    while True:
        try:
            stream = await js.stream_info(QUEUE_NAME)
            length = stream.state.messages
            print(f"Queue length: {length}")
            if length > THRESHOLD and current_replicas < 3:
                scale(3)
            elif length == 0 and current_replicas > 1:
                scale(1)
        except:
            pass
        await asyncio.sleep(CHECK_INTERVAL)

asyncio.run(main())
