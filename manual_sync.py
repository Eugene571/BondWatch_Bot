# manual_sync.py
import asyncio
import logging
from bonds_get.nightly_sync import perform_nightly_sync

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def main():
    await perform_nightly_sync()


if __name__ == "__main__":
    asyncio.run(main())
