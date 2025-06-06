import asyncio
from masedb import AsyncMaseDBClient

async def main():
    async with AsyncMaseDBClient(api_key="your_api_key") as client:
        # Создаем коллекцию
        await client.create_collection("users", "Collection for user data")
        
        # Создаем несколько документов параллельно
        documents = [
            {"name": "John Doe", "email": "john@example.com"},
            {"name": "Jane Smith", "email": "jane@example.com"},
            {"name": "Bob Johnson", "email": "bob@example.com"}
        ]
        
        # Пакетное создание документов
        tasks = [
            client.create_document("users", doc)
            for doc in documents
        ]
        results = await asyncio.gather(*tasks)
        
        # Получаем статистику
        stats = await client.get_stats()
        print(f"Database stats: {stats}")

# Запускаем асинхронный код
asyncio.run(main())