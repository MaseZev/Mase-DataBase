"""
Basic usage example of MaseDB client.

This example demonstrates:
- Creating and managing collections
- Basic CRUD operations with documents
- Using MongoDB-style query operators
- Error handling
"""

from masedb import MaseDBClient
from masedb.exceptions import MaseDBError

def main():
    # Initialize client
    client = MaseDBClient(api_key="your_api_key")
    
    try:
        # Create a collection
        client.create_collection("users", "User collection for examples")
        print("Collection 'users' created successfully")
        
        # Insert documents
        user1 = {
            "name": "John Doe",
            "age": 30,
            "email": "john@example.com",
            "tags": ["customer", "active"],
            "address": {
                "city": "New York",
                "country": "USA"
            }
        }
        
        user2 = {
            "name": "Jane Smith",
            "age": 25,
            "email": "jane@example.com",
            "tags": ["customer", "inactive"],
            "address": {
                "city": "London",
                "country": "UK"
            }
        }
        
        # Insert documents
        doc1 = client.insert_one("users", user1)
        doc2 = client.insert_one("users", user2)
        print(f"Inserted documents with IDs: {doc1['id']}, {doc2['id']}")
        
        # Find documents using query operators
        active_users = client.list_documents("users", {
            "tags": "active",
            "age": {"$gt": 25}
        })
        print(f"Found {len(active_users)} active users over 25")
        
        # Update a document
        client.update_document("users", doc1["id"], {
            "$set": {"status": "verified"},
            "$inc": {"visits": 1},
            "$push": {"tags": "verified"}
        })
        print(f"Updated document {doc1['id']}")
        
        # Get a single document
        user = client.find_one("users", {"email": "john@example.com"})
        print(f"Found user: {user}")
        
        # Delete a document
        client.delete_document("users", doc2["id"])
        print(f"Deleted document {doc2['id']}")
        
        # Get collection statistics
        stats = client.get_collection("users")
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
            client.delete_collection("users")
            print("Collection 'users' deleted")
        except MaseDBError:
            pass

if __name__ == "__main__":
    main() 