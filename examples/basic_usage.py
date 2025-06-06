"""
Basic usage example for MaseDB Python Client.

This example demonstrates:
- Creating and managing collections
- Basic CRUD operations with documents
- Using MongoDB-style query operators
- Error handling
"""

from masedb import MaseDBClient
from masedb.exceptions import MaseDBError, BadRequestError, UnauthorizedError

def main():
    # Initialize client with your API key
    client = MaseDBClient(api_key="your_api_key")
    
    try:
        # Create a collection
        print("\n1. Creating a collection...")
        collection = client.create_collection("users", "Collection for user data")
        print(f"Collection created: {collection}")
        
        # Create documents
        print("\n2. Creating documents...")
        documents = [
            {
                "name": "John Doe",
                "email": "john@example.com",
                "age": 30,
                "status": "active",
                "tags": ["verified", "premium"],
                "last_login": "2024-03-20T10:00:00Z"
            },
            {
                "name": "Jane Smith",
                "email": "jane@example.com",
                "age": 25,
                "status": "pending",
                "tags": ["new"],
                "last_login": "2024-03-19T15:30:00Z"
            },
            {
                "name": "Bob Johnson",
                "email": "bob@example.com",
                "age": 35,
                "status": "active",
                "tags": ["verified"],
                "last_login": "2024-03-20T09:15:00Z"
            }
        ]
        
        for doc in documents:
            result = client.create_document("users", doc)
            print(f"Document created: {result}")
        
        # List all documents
        print("\n3. Listing all documents...")
        all_docs = client.list_documents("users")
        print(f"Found {len(all_docs)} documents")
        
        # Query documents using MongoDB-style operators
        print("\n4. Querying documents with MongoDB-style operators...")
        
        # Example 1: Basic comparison operators
        print("\nExample 1: Age > 25")
        age_query = {"age": {"$gt": 25}}
        results = client.list_documents("users", age_query)
        print(f"Found {len(results)} documents with age > 25")
        
        # Example 2: Multiple conditions
        print("\nExample 2: Active users with premium tag")
        complex_query = {
            "status": "active",
            "tags": {"$in": ["premium"]}
        }
        results = client.list_documents("users", complex_query)
        print(f"Found {len(results)} active premium users")
        
        # Example 3: Logical operators
        print("\nExample 3: Users with email or phone")
        logical_query = {
            "$or": [
                {"email": {"$exists": True}},
                {"phone": {"$exists": True}}
            ]
        }
        results = client.list_documents("users", logical_query)
        print(f"Found {len(results)} users with email or phone")
        
        # Example 4: Update with operators
        print("\nExample 4: Updating document with operators")
        update = {
            "$set": {"status": "active"},
            "$inc": {"age": 1},
            "$push": {"tags": "updated"},
            "$currentDate": {"lastModified": True}
        }
        result = client.update_document("users", results[0]["_id"], update)
        print(f"Update result: {result}")
        
        # Example 5: Delete document
        print("\nExample 5: Deleting a document")
        result = client.delete_document("users", results[0]["_id"])
        print(f"Delete result: {result}")
        
        # Clean up
        print("\n6. Cleaning up...")
        client.delete_collection("users")
        print("Collection deleted")
        
    except BadRequestError as e:
        print(f"Invalid request: {e}")
    except UnauthorizedError as e:
        print(f"Authentication failed: {e}")
    except MaseDBError as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    main() 