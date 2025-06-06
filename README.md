# MaseDB Python Client / Клиент MaseDB для Python

<div align="center">

![MaseDB Logo](https://masedb.maseai.online/static/logo.png)

[![PyPI version](https://badge.fury.io/py/masedb.svg)](https://badge.fury.io/py/masedb)
[![Python Versions](https://img.shields.io/pypi/pyversions/masedb.svg)](https://pypi.org/project/masedb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://masedb.maseai.online/documentation)
[![GitHub stars](https://img.shields.io/github/stars/maseai/masedb.svg)](https://github.com/maseai/masedb/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/maseai/masedb.svg)](https://github.com/maseai/masedb/network)
[![GitHub issues](https://img.shields.io/github/issues/maseai/masedb.svg)](https://github.com/maseai/masedb/issues)
[![GitHub pull requests](https://img.shields.io/github/issues-pr/maseai/masedb.svg)](https://github.com/maseai/masedb/pulls)

[English](#english) | [Русский](#russian)

</div>

## Quick Links / Быстрые ссылки

- 🌐 [Website / Сайт](https://masedb.maseai.online/)
- 📚 [Documentation / Документация](https://masedb.maseai.online/documentation)
- 💬 [Support / Поддержка](https://masedb.maseai.online/support)
- 📧 [Email / Почта](mailto:admin@maseai.online)
- 📦 [PyPI Package](https://pypi.org/project/masedb/)
- 📂 [Examples / Примеры](#examples)
- ⭐ [GitHub Repository](https://github.com/maseai/masedb)

## Table of Contents / Содержание

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

---

<a name="english"></a>
# English

## Overview

MaseDB Python Client is a powerful library for interacting with the Mase Database API. It provides both synchronous and asynchronous interfaces for database operations, supporting MongoDB-style queries, transactions, indexing, and comprehensive error handling.

## Features

- ✨ Synchronous and asynchronous client interfaces
- 🔍 MongoDB-style query operators
- 🔄 Transaction support
- 📊 Index management
- 🛡️ Comprehensive error handling
- 📈 Statistics and monitoring
- 📦 Batch operations support
- 📝 Type hints and documentation

## Installation

### From PyPI
```bash
pip install masedb
```

### From GitHub
```bash
# Clone the repository
git clone https://github.com/maseai/masedb.git

# Navigate to the project directory
cd masedb

# Install in development mode
pip install -e .
```

## Quick Start

```python
from masedb import MaseDBClient

# Initialize client with your API key
client = MaseDBClient(api_key="your_api_key")

# Create a collection
client.create_collection("users", "Collection for user data")

# Create a document
document = {
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
}
result = client.create_document("users", document)
```

## Examples

Check out the `examples` directory for complete working examples:

- `basic_usage.py` - Basic CRUD operations and collection management
- `advanced_queries.py` - Complex MongoDB-style query operators and array operations
- `transactions.py` - Transaction management and money transfer examples
- `async_usage.py` - Asynchronous operations with asyncio

Each example demonstrates different aspects of the library:

### Basic Usage
```python
# basic_usage.py demonstrates:
- Creating and managing collections
- Basic CRUD operations with documents
- Using MongoDB-style query operators
- Error handling
```

### Advanced Queries
```python
# advanced_queries.py demonstrates:
- Complex MongoDB-style query operators
- Array operations
- Logical operators
- Type checking
- Regular expressions
- Nested document queries
```

### Transactions
```python
# transactions.py demonstrates:
- Starting and managing transactions
- Performing multiple operations within a transaction
- Handling transaction rollbacks
- Error handling in transactions
- Transaction status monitoring
```

### Async Usage
```python
# async_usage.py demonstrates:
- Using the async client with asyncio
- Managing collections asynchronously
- Performing CRUD operations asynchronously
- Using transactions
- Error handling in async context
```

To run an example:

```bash
python examples/basic_usage.py
```

## API Reference

### Collections

```python
# List all collections
collections = client.list_collections()

# Create a new collection
client.create_collection("users", "Collection for user data")

# Get collection details
collection = client.get_collection("users")

# Delete a collection
client.delete_collection("users")
```

### Documents

```python
# List all documents in a collection
documents = client.list_documents("users")

# Create a new document
document = {
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
}
result = client.create_document("users", document)

# Get a specific document
document = client.get_document("users", "document_id")

# Update a document
updated_doc = {
    "name": "John Doe",
    "email": "john.doe@example.com",
    "age": 31
}
client.update_document("users", "document_id", updated_doc)

# Delete a document
client.delete_document("users", "document_id")
```

### Indexes

```python
# Create an index
client.create_index("users", ["email", "age"])

# List all indexes
indexes = client.list_indexes("users")
```

### Transactions

```python
# Start a transaction
transaction = client.start_transaction()
transaction_id = transaction["transaction_id"]

# Commit a transaction
client.commit_transaction(transaction_id)

# Rollback a transaction
client.rollback_transaction(transaction_id)

# Get transaction status
status = client.get_transaction_status(transaction_id)
```

### Statistics

```python
# Get database statistics
stats = client.get_stats()

# Get detailed statistics (admin only)
detailed_stats = client.get_detailed_stats()
```

### Batch Operations

```python
def batch_create_documents(client, collection_name, documents):
    """Create multiple documents in parallel"""
    return [client.create_document(collection_name, doc) for doc in documents]

# Usage
documents = [
    {"name": "John Doe", "email": "john@example.com"},
    {"name": "Jane Smith", "email": "jane@example.com"},
    {"name": "Bob Johnson", "email": "bob@example.com"}
]

results = batch_create_documents(client, "users", documents)
```

## Error Handling

The library provides specific exceptions for different error cases:

```python
from masedb import (
    MaseDBError,
    BadRequestError,
    UnauthorizedError,
    ForbiddenError,
    NotFoundError,
    ConflictError,
    ValidationError,
    RateLimitError,
    InternalError,
    ServiceUnavailableError
)

try:
    client.create_collection("users")
except UnauthorizedError as e:
    print(f"Authentication failed: {e.message}")
except ValidationError as e:
    print(f"Validation error: {e.message}")
    print(f"Details: {e.details}")
except MaseDBError as e:
    print(f"An error occurred: {e.message}")
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](https://masedb.maseai.online/documentation/contributing) for details.

### Development Setup

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/your-username/masedb.git
   ```
3. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```
5. Run tests:
   ```bash
   pytest
   ```

### Pull Request Process

1. Update the README.md with details of changes if needed
2. Update the documentation if needed
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License

---

<a name="russian"></a>
# Русский

## Обзор

MaseDB Python Client - это мощная библиотека для взаимодействия с API базы данных Mase. Она предоставляет как синхронный, так и асинхронный интерфейсы для операций с базой данных, поддерживает MongoDB-подобные запросы, транзакции, индексацию и комплексную обработку ошибок.

## Возможности

- ✨ Синхронный и асинхронный интерфейсы клиента
- 🔍 MongoDB-подобные операторы запросов
- 🔄 Поддержка транзакций
- 📊 Управление индексами
- 🛡️ Комплексная обработка ошибок
- 📈 Статистика и мониторинг
- 📦 Поддержка пакетных операций
- 📝 Подсказки типов и документация

## Установка

### Из PyPI
```bash
pip install masedb
```

### Из GitHub
```bash
# Клонировать репозиторий
git clone https://github.com/maseai/masedb.git

# Перейти в директорию проекта
cd masedb

# Установить в режиме разработки
pip install -e .
```

## Быстрый старт

```python
from masedb import MaseDBClient

# Инициализация клиента с вашим API ключом
client = MaseDBClient(api_key="ваш_api_ключ")

# Создание коллекции
client.create_collection("users", "Коллекция для данных пользователей")

# Создание документа
document = {
    "name": "Иван Иванов",
    "email": "ivan@example.com",
    "age": 30
}
result = client.create_document("users", document)
```

## Примеры

В директории `examples` вы найдете полные рабочие примеры:

- `basic_usage.py` - Базовые операции CRUD и управление коллекциями
- `advanced_queries.py` - Сложные MongoDB-подобные операторы запросов и операции с массивами
- `transactions.py` - Управление транзакциями и примеры денежных переводов
- `async_usage.py` - Асинхронные операции с asyncio

Каждый пример демонстрирует различные аспекты библиотеки:

### Базовое использование
```python
# basic_usage.py демонстрирует:
- Создание и управление коллекциями
- Базовые операции CRUD с документами
- Использование MongoDB-подобных операторов запросов
- Обработку ошибок
```

### Расширенные запросы
```python
# advanced_queries.py демонстрирует:
- Сложные MongoDB-подобные операторы запросов
- Операции с массивами
- Логические операторы
- Проверку типов
- Регулярные выражения
- Запросы к вложенным документам
```

### Транзакции
```python
# transactions.py демонстрирует:
- Начало и управление транзакциями
- Выполнение множественных операций в транзакции
- Обработку откатов транзакций
- Обработку ошибок в транзакциях
- Мониторинг статуса транзакций
```

### Асинхронное использование
```python
# async_usage.py демонстрирует:
- Использование асинхронного клиента с asyncio
- Асинхронное управление коллекциями
- Асинхронное выполнение операций CRUD
- Использование транзакций
- Обработку ошибок в асинхронном контексте
```

Для запуска примера:

```bash
python examples/basic_usage.py
```

## Справочник API

### Коллекции

```python
# Получение списка всех коллекций
collections = client.list_collections()

# Создание новой коллекции
client.create_collection("users", "Коллекция для данных пользователей")

# Получение информации о коллекции
collection = client.get_collection("users")

# Удаление коллекции
client.delete_collection("users")
```

### Документы

```python
# Получение всех документов в коллекции
documents = client.list_documents("users")

# Создание нового документа
document = {
    "name": "Иван Иванов",
    "email": "ivan@example.com",
    "age": 30
}
result = client.create_document("users", document)

# Получение конкретного документа
document = client.get_document("users", "document_id")

# Обновление документа
updated_doc = {
    "name": "Иван Иванов",
    "email": "ivan.ivanov@example.com",
    "age": 31
}
client.update_document("users", "document_id", updated_doc)

# Удаление документа
client.delete_document("users", "document_id")
```

### Индексы

```python
# Создание индекса
client.create_index("users", ["email", "age"])

# Получение списка всех индексов
indexes = client.list_indexes("users")
```

### Транзакции

```python
# Начало транзакции
transaction = client.start_transaction()
transaction_id = transaction["transaction_id"]

# Подтверждение транзакции
client.commit_transaction(transaction_id)

# Откат транзакции
client.rollback_transaction(transaction_id)

# Получение статуса транзакции
status = client.get_transaction_status(transaction_id)
```

### Статистика

```python
# Получение статистики базы данных
stats = client.get_stats()

# Получение детальной статистики (только для администраторов)
detailed_stats = client.get_detailed_stats()
```

### Пакетные операции

```python
def batch_create_documents(client, collection_name, documents):
    """Создание нескольких документов параллельно"""
    return [client.create_document(collection_name, doc) for doc in documents]

# Использование
documents = [
    {"name": "Иван Иванов", "email": "ivan@example.com"},
    {"name": "Мария Петрова", "email": "maria@example.com"},
    {"name": "Алексей Сидоров", "email": "alexey@example.com"}
]

results = batch_create_documents(client, "users", documents)
```

## Обработка ошибок

Библиотека предоставляет специфические исключения для различных случаев ошибок:

```python
from masedb import (
    MaseDBError,
    BadRequestError,
    UnauthorizedError,
    ForbiddenError,
    NotFoundError,
    ConflictError,
    ValidationError,
    RateLimitError,
    InternalError,
    ServiceUnavailableError
)

try:
    client.create_collection("users")
except UnauthorizedError as e:
    print(f"Ошибка аутентификации: {e.message}")
except ValidationError as e:
    print(f"Ошибка валидации: {e.message}")
    print(f"Детали: {e.details}")
except MaseDBError as e:
    print(f"Произошла ошибка: {e.message}")
```

## Участие в разработке

Мы приветствуем ваш вклад! Подробности смотрите в [Руководстве по участию в разработке](https://masedb.maseai.online/documentation/contributing).

### Настройка окружения разработки

1. Форкните репозиторий
2. Клонируйте ваш форк:
   ```bash
   git clone https://github.com/ваш-username/masedb.git
   ```
3. Создайте виртуальное окружение:
   ```bash
   python -m venv venv
   source venv/bin/activate  # На Windows: venv\Scripts\activate
   ```
4. Установите зависимости для разработки:
   ```bash
   pip install -e ".[dev]"
   ```
5. Запустите тесты:
   ```bash
   pytest
   ```

### Процесс создания Pull Request

1. Обновите README.md при необходимости
2. Обновите документацию при необходимости
3. Добавьте тесты для новой функциональности
4. Убедитесь, что все тесты проходят
5. Отправьте pull request

## Лицензия

MIT License 
