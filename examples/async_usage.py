"""
Asynchronous usage example of MaseDB client.

This example demonstrates:
- Using the async client with asyncio
- Managing collections asynchronously
- Performing CRUD operations asynchronously
- Using transactions
- Error handling in async context
"""

import asyncio
from masedb import AsyncMaseDBClient
from masedb.exceptions import MaseDBError

async def main():
    # Initialize async client
    async with AsyncMaseDBClient(api_key="your_api_key") as client:
        try:
            # Create a collection
            await client.create_collection("products", "Product collection for examples")
            print("Collection 'products' created successfully")
            
            # Start a transaction
            transaction = await client.start_transaction()
            print(f"Started transaction: {transaction['transaction_id']}")
            
            try:
                # Insert products within transaction
                product1 = {
                    "name": "Laptop",
                    "price": 999.99,
                    "category": "Electronics",
                    "in_stock": True,
                    "specs": {
                        "cpu": "Intel i7",
                        "ram": "16GB",
                        "storage": "512GB SSD"
                    }
                }
                
                product2 = {
                    "name": "Smartphone",
                    "price": 699.99,
                    "category": "Electronics",
                    "in_stock": True,
                    "specs": {
                        "cpu": "Snapdragon 888",
                        "ram": "8GB",
                        "storage": "128GB"
                    }
                }
                
                # Insert documents
                doc1 = await client.insert_one("products", product1)
                doc2 = await client.insert_one("products", product2)
                print(f"Inserted products with IDs: {doc1['id']}, {doc2['id']}")
                
                # Create an index
                await client.create_index("products", ["category", "price"])
                print("Created index on category and price")
                
                # Find products using query operators
                electronics = await client.list_documents("products", {
                    "category": "Electronics",
                    "price": {"$lt": 1000},
                    "in_stock": True
                })
                print(f"Found {len(electronics)} electronics products under $1000")
                
                # Update a product
                await client.update_document("products", doc1["id"], {
                    "$set": {"in_stock": False},
                    "$inc": {"price": 50},
                    "$currentDate": {"last_updated": True}
                })
                print(f"Updated product {doc1['id']}")
                
                # Get product details
                product = await client.find_one("products", {
                    "name": "Smartphone",
                    "in_stock": True
                })
                print(f"Found product: {product}")
                
                # Get transaction status
                status = await client.get_transaction_status(transaction["transaction_id"])
                print(f"Transaction status: {status}")
                
                # Commit transaction
                await client.commit_transaction(transaction["transaction_id"])
                print("Transaction committed successfully")
                
            except MaseDBError as e:
                # Rollback transaction on error
                await client.rollback_transaction(transaction["transaction_id"])
                print(f"Transaction rolled back due to error: {e.message}")
                raise
            
            # Get collection statistics
            stats = await client.get_collection("products")
            print(f"Collection stats: {stats}")
            
        except MaseDBError as e:
            print(f"Error: {e.message}")
            if e.code:
                print(f"Error code: {e.code}")
            if e.details:
                print(f"Error details: {e.details}")
        finally:
            # Clean up
            try:
                await client.delete_collection("products")
                print("Collection 'products' deleted")
            except MaseDBError:
                pass

if __name__ == "__main__":
    asyncio.run(main()) 