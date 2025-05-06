import asyncio

import httpx


async def is_bond(isin: str) -> bool:
    """Асинхронно проверяет, является ли бумага облигацией по параметру GROUP."""
    url = f"https://iss.moex.com/iss/securities/{isin}.json"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

            # Ищем параметр GROUP со значением stock_bonds
            for item in data.get('description', {}).get('data', []):
                if len(item) >= 3 and item[0] == 'GROUP' and item[2] == 'stock_bonds':
                    return True
            return False

    except httpx.HTTPStatusError as e:
        print(f"HTTP error: {e}")
        return False
    except (KeyError, IndexError, ValueError, TypeError) as e:
        print(f"Data processing error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


async def main():
    print(await is_bond("RU000A105740"))  # True


if __name__ == "__main__":
    asyncio.run(main())
