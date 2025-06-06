# MaseDB Examples / Примеры MaseDB

<div align="center">

[English](#english) | [Русский](#russian)

</div>

---

<a name="english"></a>
# English

## Overview

This directory contains example scripts demonstrating various features and use cases of the MaseDB Python Client. Each example is designed to showcase different aspects of the library's functionality.

## Available Examples

### 🚀 Basic Usage (`basic_usage.py`)
- ✨ Creating and managing collections
- 📝 Basic CRUD operations with documents
- 🔍 Using MongoDB-style query operators:
  - Comparison: $eq, $ne, $gt, $gte, $lt, $lte
  - Array: $in, $nin
  - Existence: $exists
  - Type: $type
  - Regex: $regex
  - Logical: $or, $and, $not, $nor
- ⚠️ Error handling

### 🔬 Advanced Queries (`advanced_queries.py`)
- 🎯 Complex MongoDB-style query operators
- 📊 Array operations
- 🔢 Logical operators
- 📋 Type checking
- 🔤 Regular expressions
- 📑 Nested document queries
- 🔄 Update operators:
  - $set: Set field values
  - $inc: Increment numeric values
  - $mul: Multiply numeric values
  - $rename: Rename fields
  - $unset: Remove fields
  - $min: Set minimum value
  - $max: Set maximum value
  - $currentDate: Set current date
  - $addToSet: Add unique elements to array
  - $push: Add elements to array
  - $pop: Remove first/last element from array
  - $pull: Remove elements from array by condition
  - $pullAll: Remove all specified elements from array

### 💾 Transactions (`transactions.py`)
- ⚡ Starting and managing transactions
- 🔄 Performing multiple operations within a transaction
- ↩️ Handling transaction rollbacks
- ⚠️ Error handling in transactions
- 📊 Transaction status monitoring

### ⚡ Async Usage (`async_usage.py`)
- 🔄 Using the async client with asyncio
- 📦 Managing collections asynchronously
- 📝 Performing CRUD operations asynchronously
- 💾 Using transactions
- ⚠️ Error handling in async context

## Running Examples

To run any example, use Python from the command line:

```bash
python examples/basic_usage.py
```

## Example Code Snippets

### Basic Usage
```python
from masedb import MaseDBClient

client = MaseDBClient(api_key="your_api_key")

# Create a collection
client.create_collection("users", "User collection")

# Create a document
document = {
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
}
result = client.create_document("users", document)

# Query documents
users = client.list_documents("users", {
    "age": { "$gt": 25 },
    "status": { "$in": ["active", "pending"] },
    "$or": [
        { "email": { "$exists": true } },
        { "phone": { "$exists": true } }
    ]
})
```

### Advanced Queries
```python
# Complex query with multiple operators
query = {
    "age": { "$gt": 25, "$lt": 50 },
    "status": { "$in": ["active", "pending"] },
    "tags": { "$all": ["verified", "premium"] },
    "$or": [
        { "email": { "$regex": "^john" } },
        { "phone": { "$exists": true } }
    ],
    "$and": [
        { "last_login": { "$gt": "2024-01-01" } },
        { "is_active": true }
    ]
}

# Update with multiple operators
update = {
    "$set": { "name": "John Doe" },
    "$inc": { "visits": 1 },
    "$push": { "tags": { "$each": ["new", "user"] } },
    "$currentDate": { "lastModified": true }
}
```

### Transactions
```python
# Start a transaction
transaction = client.start_transaction()
transaction_id = transaction["transaction_id"]

try:
    # Perform operations
    client.create_document("users", {"name": "John"})
    client.update_document("users", "doc123", {"$inc": {"balance": 100}})
    
    # Commit if successful
    client.commit_transaction(transaction_id)
except Exception as e:
    # Rollback on error
    client.rollback_transaction(transaction_id)
    raise e
```

### Async Usage
```python
import asyncio
from masedb import AsyncMaseDBClient

async def main():
    async with AsyncMaseDBClient(api_key="your_api_key") as client:
        # Create a collection
        await client.create_collection("users", "User collection")
        
        # Create a document
        document = {
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30
        }
        result = await client.create_document("users", document)
        
        # Query documents
        users = await client.list_documents("users", {
            "age": { "$gt": 25 },
            "status": { "$in": ["active", "pending"] }
        })

asyncio.run(main())
```

## Error Handling

All examples include proper error handling:

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