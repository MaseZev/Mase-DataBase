# MaseDB Python Client / Клиент MaseDB для Python

<div align="center">

![MaseDB Logo](https://masedb.maseai.online/static/logo.png)

[![PyPI version](https://badge.fury.io/py/masedb.svg)](https://badge.fury.io/py/masedb)
[![Python Versions](https://img.shields.io/pypi/pyversions/masedb.svg)](https://pypi.org/project/masedb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://masedb.maseai.online/documentation)
[![GitHub stars](https://img.shields.io/github/stars/maseai/masedb.svg)](https://github.com/MaseZev/Mase-DataBase/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/maseai/masedb.svg)](https://github.com/MaseZev/Mase-DataBase/network)
[![GitHub issues](https://img.shields.io/github/issues/maseai/masedb.svg)](https://github.com/MaseZev/Mase-DataBase/issues)
[![GitHub pull requests](https://img.shields.io/github/issues-pr/maseai/masedb.svg)](https://github.com/MaseZev/Mase-DataBase/pulls)

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

# Query documents using MongoDB-style operators
users = client.list_documents("users", {
    "age": { "$gt": 25 },
    "status": { "$in": ["active", "pending"] },
    "$or": [
        { "email": { "$exists": true } },
        { "phone": { "$exists": true } }
    ]
})
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
- Complex MongoDB-style query operators:
  - Comparison: $eq, $ne, $gt, $gte, $lt, $lte
  - Array: $in, $nin
  - Existence: $exists
  - Type: $type
  - Regex: $regex
  - Logical: $or, $and, $not, $nor
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

# Get detailed collection list
detailed_collections = client.list_collections_detailed()

# Create a new collection
client.create_collection("users", "Collection for user data")

# Get collection details
collection = client.get_collection("users")

# Delete a collection
client.delete_collection("users")
```

### Documents

```python
# List documents with MongoDB-style queries
documents = client.list_documents("users", {
    "age": { "$gt": 25 },
    "status": { "$in": ["active", "pending"] },
    "$or": [
        { "email": { "$exists": true } },
        { "phone": { "$exists": true } }
    ]
}, sort={"age": 1, "name": -1}, limit=10)

# Create a new document
document = {
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
}
result = client.create_document("users", document)

# Get a specific document
document = client.get_document("users", "document_id")

# Update a document with MongoDB-style operators
update = {
    "$set": { "name": "John Doe" },
    "$inc": { "visits": 1 },
    "$push": { "tags": { "$each": ["new", "user"] } },
    "$currentDate": { "lastModified": true }
}
client.update_document("users", "document_id", update)

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

### MongoDB-style Operators

#### Query Operators
- Comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`
- Array: `$in`, `$nin`
- Existence: `$exists`
- Type: `$type`
- Regex: `$regex`
- Logical: `$or`, `$and`, `$not`, `$nor`

#### Update Operators
- `$set`: Set field values
- `$inc`: Increment numeric values
- `$mul`: Multiply numeric values
- `$rename`: Rename fields
- `$unset`: Remove fields
- `$min`: Set minimum value
- `$max`: Set maximum value
- `$currentDate`: Set current date
- `$addToSet`: Add unique elements to array
- `$push`: Add elements to array
- `$pop`: Remove first/last element from array
- `$pull`: Remove elements from array by condition
- `$pullAll`: Remove all specified elements from array

### Error Handling

The client provides comprehensive error handling with specific exception types for different error scenarios:

```python
from masedb.exceptions import MaseDBError, BadRequestError, UnauthorizedError

try:
    client.create_document("users", {"name": "John"})
except BadRequestError as e:
    print(f"Invalid request: {e}")
except UnauthorizedError as e:
    print(f"Authentication failed: {e}")
except MaseDBError as e:
    print(f"Database error: {e}")
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

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