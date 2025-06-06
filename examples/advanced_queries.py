"""
Advanced query examples for MaseDB client.

This example demonstrates:
- Complex MongoDB-style query operators
- Array operations
- Logical operators
- Type checking
- Regular expressions
- Nested document queries
"""

from masedb import MaseDBClient
from masedb.exceptions import MaseDBError

def main():
    client = MaseDBClient(api_key="your_api_key")
    
    try:
        # Create a collection
        client.create_collection("orders", "Order collection for advanced queries")
        print("Collection 'orders' created successfully")
        
        # Insert sample orders
        orders = [
            {
                "order_id": "ORD001",
                "customer": {
                    "name": "John Doe",
                    "email": "john.doe@example.com",
                    "phone": "+1-555-123-4567"
                },
                "items": [
                    {"product": "Laptop", "quantity": 1, "price": 999.99},
                    {"product": "Mouse", "quantity": 2, "price": 29.99}
                ],
                "total": 1059.97,
                "status": "completed",
                "created_at": "2024-03-20T10:00:00Z",
                "tags": ["electronics", "priority", "express"]
            },
            {
                "order_id": "ORD002",
                "customer": {
                    "name": "Jane Smith",
                    "email": "jane.smith@example.com",
                    "phone": "+44-20-1234-5678"
                },
                "items": [
                    {"product": "Smartphone", "quantity": 1, "price": 699.99},
                    {"product": "Case", "quantity": 1, "price": 19.99}
                ],
                "total": 719.98,
                "status": "pending",
                "created_at": "2024-03-21T15:30:00Z",
                "tags": ["electronics", "standard"]
            }
        ]
        
        for order in orders:
            client.insert_one("orders", order)
        print("Inserted sample orders")
        
        # Example 1: Complex logical operators
        print("\nExample 1: Complex logical operators")
        orders = client.list_documents("orders", {
            "$or": [
                {"total": {"$gt": 1000}},
                {
                    "$and": [
                        {"status": "pending"},
                        {"tags": "priority"}
                    ]
                }
            ]
        })
        print(f"Found {len(orders)} orders matching complex conditions")
        
        # Example 2: Array operations
        print("\nExample 2: Array operations")
        orders = client.list_documents("orders", {
            "items": {
                "$elemMatch": {
                    "price": {"$gt": 500},
                    "quantity": {"$gt": 0}
                }
            }
        })
        print(f"Found {len(orders)} orders with items over $500")
        
        # Example 3: Regular expressions
        print("\nExample 3: Regular expressions")
        orders = client.list_documents("orders", {
            "customer.email": {"$regex": "^jane", "$options": "i"}
        })
        print(f"Found {len(orders)} orders from customers with email starting with 'jane'")
        
        # Example 4: Nested document queries
        print("\nExample 4: Nested document queries")
        orders = client.list_documents("orders", {
            "customer.phone": {"$regex": "^\\+44"}
        })
        print(f"Found {len(orders)} orders from UK customers")
        
        # Example 5: Type checking
        print("\nExample 5: Type checking")
        orders = client.list_documents("orders", {
            "total": {"$type": "number"}
        })
        print(f"Found {len(orders)} orders with numeric total")
        
        # Example 6: Array element matching
        print("\nExample 6: Array element matching")
        orders = client.list_documents("orders", {
            "tags": {
                "$all": ["electronics"],
                "$size": 3
            }
        })
        print(f"Found {len(orders)} orders with exactly 3 tags including 'electronics'")
        
        # Example 7: Combined operators
        print("\nExample 7: Combined operators")
        orders = client.list_documents("orders", {
            "$and": [
                {"status": {"$in": ["completed", "pending"]}},
                {"total": {"$gt": 500, "$lt": 2000}},
                {"tags": {"$exists": True}},
                {"customer.name": {"$regex": "^J", "$options": "i"}}
            ]
        })
        print(f"Found {len(orders)} orders matching combined conditions")
        
    except MaseDBError as e:
        print(f"Error: {e.message}")
        if e.code:
            print(f"Error code: {e.code}")
        if e.details:
            print(f"Error details: {e.details}")
    finally:
        # Clean up
        try:
            client.delete_collection("orders")
            print("\nCollection 'orders' deleted")
        except MaseDBError:
            pass

if __name__ == "__main__":
    main() 