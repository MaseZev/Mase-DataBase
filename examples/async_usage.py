"""
Async usage example for MaseDB Python Client.

This example demonstrates:
- Using the async client with asyncio
- Creating and managing collections asynchronously
- Performing CRUD operations asynchronously
- Using MongoDB-style operators in async context
- Error handling in async operations
"""

import asyncio
from datetime import datetime
from masedb import AsyncMaseDBClient
from masedb.exceptions import MaseDBError, BadRequestError, UnauthorizedError

async def main():
    # Initialize async client with your API key
    client = AsyncMaseDBClient(api_key="your_api_key")
    
    try:
        # Create collections
        print("\n1. Creating collections...")
        await client.create_collection("users", "Collection for user profiles")
        await client.create_collection("posts", "Collection for user posts")
        print("Collections created successfully")
        
        # Create initial users
        print("\n2. Creating initial users...")
        users = [
            {
                "username": "john_doe",
                "email": "john@example.com",
                "age": 30,
                "status": "active",
                "tags": ["premium", "verified"],
                "last_login": datetime.utcnow().isoformat()
            },
            {
                "username": "jane_smith",
                "email": "jane@example.com",
                "age": 25,
                "status": "active",
                "tags": ["verified"],
                "last_login": datetime.utcnow().isoformat()
            }
        ]
        
        for user in users:
            result = await client.create_document("users", user)
            print(f"User created: {result}")
        
        # Example 1: Basic async operations
        print("\nExample 1: Basic async operations")
        try:
            # Create a post
            post = {
                "author": "john_doe",
                "title": "Hello World",
                "content": "This is my first post",
                "tags": ["introduction", "hello"],
                "created_at": datetime.utcnow().isoformat()
            }
            result = await client.create_document("posts", post)
            print(f"Post created: {result}")
            
            # Update user's last login
            await client.update_document("users", "john_doe", {
                "$set": {"last_login": datetime.utcnow().isoformat()},
                "$addToSet": {"tags": "poster"}
            })
            
            # Get user with posts
            user = await client.find_one("users", {"username": "john_doe"})
            user_posts = await client.list_documents("posts", {"author": "john_doe"})
            print(f"User: {user}")
            print(f"User posts: {user_posts}")
            
        except Exception as e:
            print(f"Error during basic operations: {e}")
        
        # Example 2: Complex async queries
        print("\nExample 2: Complex async queries")
        try:
            # Find active users with specific tags
            active_users = await client.list_documents("users", {
                "status": "active",
                "tags": {"$all": ["verified"]},
                "age": {"$gte": 25}
            })
            print(f"Active verified users: {active_users}")
            
            # Find posts with specific criteria
            recent_posts = await client.list_documents("posts", {
                "$or": [
                    {"tags": {"$in": ["introduction"]}},
                    {"author": {"$in": ["john_doe", "jane_smith"]}}
                ],
                "created_at": {"$exists": True}
            })
            print(f"Recent posts: {recent_posts}")
            
        except Exception as e:
            print(f"Error during complex queries: {e}")
        
        # Example 3: Batch async operations
        print("\nExample 3: Batch async operations")
        try:
            # Create multiple posts concurrently
            posts = [
                {
                    "author": "jane_smith",
                    "title": "My First Post",
                    "content": "Hello everyone!",
                    "tags": ["greeting"],
                    "created_at": datetime.utcnow().isoformat()
                },
                {
                    "author": "john_doe",
                    "title": "Follow-up Post",
                    "content": "This is a follow-up",
                    "tags": ["update"],
                    "created_at": datetime.utcnow().isoformat()
                }
            ]
            
            # Create posts concurrently
            create_tasks = [client.create_document("posts", post) for post in posts]
            results = await asyncio.gather(*create_tasks)
            print(f"Created posts: {results}")
            
            # Update users concurrently
            update_tasks = [
                client.update_document("users", "john_doe", {
                    "$inc": {"post_count": 1},
                    "$currentDate": {"last_modified": True}
                }),
                client.update_document("users", "jane_smith", {
                    "$inc": {"post_count": 1},
                    "$currentDate": {"last_modified": True}
                })
            ]
            await asyncio.gather(*update_tasks)
            print("Updated user post counts")
            
        except Exception as e:
            print(f"Error during batch operations: {e}")
        
        # Clean up
        print("\n4. Cleaning up...")
        await client.delete_collection("users")
        await client.delete_collection("posts")
        print("Collections deleted")
        
    except BadRequestError as e:
        print(f"Invalid request: {e}")
    except UnauthorizedError as e:
        print(f"Authentication failed: {e}")
    except MaseDBError as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 