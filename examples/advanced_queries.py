"""
Advanced queries example for MaseDB Python Client.

This example demonstrates:
- Complex MongoDB-style query operators
- Array operations
- Logical operators
- Type checking
- Regular expressions
- Nested document queries
- Update operators
"""

from masedb import MaseDBClient
from masedb.exceptions import MaseDBError, BadRequestError, UnauthorizedError
from datetime import datetime

def main():
    # Initialize client with your API key
    client = MaseDBClient(api_key="your_api_key")
    
    try:
        # Create a collection
        print("\n1. Creating a collection...")
        collection = client.create_collection("products", "Collection for product data")
        print(f"Collection created: {collection}")
        
        # Create sample products
        print("\n2. Creating sample products...")
        products = [
            {
                "name": "Laptop Pro X1",
                "price": 1299.99,
                "category": "electronics",
                "tags": ["laptop", "premium", "new"],
                "specs": {
                    "cpu": "Intel i7",
                    "ram": 16,
                    "storage": "512GB SSD"
                },
                "stock": 50,
                "ratings": [4.5, 5.0, 4.8],
                "created_at": "2024-01-15T10:00:00Z",
                "updated_at": "2024-03-20T15:30:00Z"
            },
            {
                "name": "Smartphone Y2",
                "price": 899.99,
                "category": "electronics",
                "tags": ["phone", "5G", "camera"],
                "specs": {
                    "cpu": "Snapdragon 8",
                    "ram": 8,
                    "storage": "256GB"
                },
                "stock": 100,
                "ratings": [4.2, 4.0, 4.5],
                "created_at": "2024-02-01T09:00:00Z",
                "updated_at": "2024-03-19T14:20:00Z"
            },
            {
                "name": "Wireless Earbuds",
                "price": 199.99,
                "category": "audio",
                "tags": ["wireless", "bluetooth", "noise-cancelling"],
                "specs": {
                    "battery": "8 hours",
                    "connectivity": "Bluetooth 5.0"
                },
                "stock": 200,
                "ratings": [4.8, 4.9, 5.0],
                "created_at": "2024-03-01T11:00:00Z",
                "updated_at": "2024-03-20T10:15:00Z"
            }
        ]
        
        for product in products:
            result = client.create_document("products", product)
            print(f"Product created: {result}")
        
        # Example 1: Complex comparison operators
        print("\nExample 1: Products in price range with high stock")
        price_query = {
            "price": {
                "$gt": 500,
                "$lt": 1500
            },
            "stock": {"$gte": 50}
        }
        results = client.list_documents("products", price_query)
        print(f"Found {len(results)} products in price range with high stock")
        
        # Example 2: Array operations
        print("\nExample 2: Products with specific tags")
        array_query = {
            "tags": {
                "$all": ["new", "premium"],
                "$size": 3
            }
        }
        results = client.list_documents("products", array_query)
        print(f"Found {len(results)} products with specific tags")
        
        # Example 3: Nested document queries
        print("\nExample 3: Products with specific specs")
        nested_query = {
            "specs.cpu": {"$regex": "^Intel"},
            "specs.ram": {"$gte": 16}
        }
        results = client.list_documents("products", nested_query)
        print(f"Found {len(results)} products with specific specs")
        
        # Example 4: Logical operators with type checking
        print("\nExample 4: Complex logical query")
        logical_query = {
            "$and": [
                {"price": {"$type": "number"}},
                {"stock": {"$type": "number"}},
                {
                    "$or": [
                        {"category": "electronics"},
                        {"category": "audio"}
                    ]
                }
            ]
        }
        results = client.list_documents("products", logical_query)
        print(f"Found {len(results)} products matching complex criteria")
        
        # Example 5: Update with multiple operators
        print("\nExample 5: Complex update operation")
        update = {
            "$set": {
                "status": "in_stock",
                "last_checked": datetime.utcnow().isoformat()
            },
            "$inc": {
                "stock": -1,
                "times_checked": 1
            },
            "$push": {
                "check_history": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "stock_level": 49
                }
            },
            "$addToSet": {
                "categories": "gadgets"
            },
            "$currentDate": {
                "last_modified": True
            }
        }
        result = client.update_document("products", results[0]["_id"], update)
        print(f"Update result: {result}")
        
        # Example 6: Array update operations
        print("\nExample 6: Array update operations")
        array_update = {
            "$pull": {
                "tags": "new"
            },
            "$push": {
                "tags": {
                    "$each": ["featured", "bestseller"],
                    "$position": 0
                }
            },
            "$pop": {
                "ratings": 1
            }
        }
        result = client.update_document("products", results[0]["_id"], array_update)
        print(f"Array update result: {result}")
        
        # Clean up
        print("\n7. Cleaning up...")
        client.delete_collection("products")
        print("Collection deleted")
        
    except BadRequestError as e:
        print(f"Invalid request: {e}")
    except UnauthorizedError as e:
        print(f"Authentication failed: {e}")
    except MaseDBError as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    main() 