"""
Asynchronous client for interacting with Mase Database API

Base URL: https://masedb.maseai.online

Authentication:
All API endpoints require authentication using API keys.

API Key Authentication:
Include the API key in the X-API-Key header:
X-API-Key: <your_api_key>

Features:
- MongoDB-style query operators
- Transaction support
- Index management
- Statistics and monitoring
- Comprehensive error handling
- Async/await support

Example:
    >>> import asyncio
    >>> from masedb import AsyncMaseDBClient
    >>> async def main():
    ...     async with AsyncMaseDBClient(api_key="your_api_key") as client:
    ...         # Create a collection
    ...         await client.create_collection("users", "User collection")
    ...         # Insert a document
    ...         await client.insert_one("users", {"name": "John", "age": 30})
    ...         # Query documents
    ...         users = await client.list_documents("users", {"age": {"$gt": 25}})
    >>> asyncio.run(main())
"""

import aiohttp
import logging
from typing import Dict, List, Optional, Union, Any, TypedDict
from datetime import datetime
from .exceptions import MaseDBError, ERROR_MAP

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('AsyncMaseDBClient')

class CollectionInfo(TypedDict):
    """Type definition for collection information"""
    name: str
    description: str
    owner_id: str
    created_at: float

class DocumentInfo(TypedDict):
    """Type definition for document information"""
    _id: str
    owner_id: str
    _created_at: float
    _updated_at: float

class IndexInfo(TypedDict):
    """Type definition for index information"""
    fields: List[str]
    created_at: float

class TransactionInfo(TypedDict):
    """Type definition for transaction information"""
    transaction_id: str
    status: str
    start_time: float
    changes_count: int

class DatabaseStats(TypedDict):
    """Type definition for database statistics"""
    collections_count: int
    documents_count: int
    data_size: int
    indexes_count: int
    collections: List[Dict]
    activity: Dict
    memory: Dict
    operations: Dict

class DetailedStats(TypedDict):
    """Type definition for detailed database statistics"""
    database_info: Dict
    shard_stats: Dict
    cache_stats: Dict

class AsyncMaseDBClient:
    """
    Asynchronous client for interacting with Mase Database API.
    
    Base URL: https://masedb.maseai.online
    
    Authentication:
    All API endpoints require authentication using API keys.
    
    API Key Authentication:
    Include the API key in the X-API-Key header:
    X-API-Key: <your_api_key>
    """
    
    def __init__(self, api_key: str, base_url: str = "https://masedb.maseai.online"):
        """
        Initialize the client with API key
        
        Args:
            api_key (str): Your Mase Database API key
            base_url (str): Base URL of the API server
        """
        self.api_key = api_key
        self.BASE_URL = base_url.rstrip('/')
        self.headers = {
            'X-API-Key': api_key,
            'Accept': 'application/json'
        }
        self._session: Optional[aiohttp.ClientSession] = None
        logger.info(f"Initialized AsyncMaseDB client with base URL: {self.BASE_URL}")
    
    async def __aenter__(self):
        """Create aiohttp session when entering context"""
        self._session = aiohttp.ClientSession(headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close aiohttp session when exiting context"""
        if self._session:
            await self._session.close()
            self._session = None
    
    @property
    def session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self._session is None:
            self._session = aiohttp.ClientSession(headers=self.headers)
        return self._session
    
    async def _handle_response(self, response: aiohttp.ClientResponse) -> Any:
        """Handle API response and raise appropriate exceptions"""
        # Log response details
        logger.debug(f"Response status code: {response.status}")
        logger.debug(f"Response headers: {response.headers}")
        
        # Get response body
        try:
            body = await response.text()
            logger.debug(f"Response body: {body}")
        except Exception as e:
            logger.error(f"Error reading response body: {str(e)}")
            body = None
            
        # Check content type
        content_type = response.headers.get('Content-Type', '')
        if response.ok:
            if not body:
                return None
            if 'application/json' not in content_type:
                logger.error(f"Expected JSON response, got {content_type}")
                raise MaseDBError(f"Invalid response format: Expected JSON, got {content_type}")
            try:
                return await response.json()
            except ValueError as e:
                logger.error(f"Error decoding JSON response: {str(e)}")
                raise MaseDBError(f"Invalid JSON response: {str(e)}")
        else:
            # Try to get error details from response
            error_message = f"HTTP {response.status}: {body or 'No response body'}"
            if 'application/json' in content_type:
                try:
                    error_data = await response.json()
                    if 'error' in error_data:
                        error_message = error_data['error'].get('message', error_message)
                        error_code = error_data['error'].get('code', 'UNKNOWN_ERROR')
                        error_details = error_data['error'].get('details', {})
                        raise MaseDBError(error_message, error_code, error_details)
                except ValueError:
                    pass
            logger.error(f"API error: {error_message}")
            raise MaseDBError(error_message)
    
    async def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """Make HTTP request to API"""
        url = f"{self.BASE_URL}{endpoint}"
        
        # Add API key to headers
        headers = kwargs.pop('headers', {})
        headers['X-API-Key'] = self.api_key
        headers['Accept'] = 'application/json'
        
        # Only set Content-Type for POST/PUT requests
        if method in ['POST', 'PUT']:
            headers['Content-Type'] = 'application/json'
        else:
            # Remove Content-Type for GET/DELETE requests
            headers.pop('Content-Type', None)
        
        # Log request details
        logger.debug(f"Making {method} request to {url}")
        logger.debug(f"Request headers: {headers}")
        if 'json' in kwargs:
            logger.debug(f"Request body: {kwargs['json']}")
            
        async with self.session.request(method, url, headers=headers, **kwargs) as response:
            return await self._handle_response(response)

    async def find_one(self, collection_name: str, query: Optional[Dict] = None) -> Optional[DocumentInfo]:
        """
        Find a single document matching the query.
        
        Args:
            collection_name (str): Name of the collection
            query (Dict, optional): Query operators to filter documents. Supports MongoDB-style operators:
                - Comparison: $eq, $ne, $gt, $gte, $lt, $lte
                - Array: $in, $nin
                - Existence: $exists
                - Type: $type
                - Regex: $regex
                - Logical: $or, $and, $not, $nor
                
        Returns:
            Optional[DocumentInfo]: First document matching the query or None if no match
            
        Example:
            >>> await client.find_one("users", {"age": {"$gt": 25}})
            {
                "_id": "doc123",
                "name": "John",
                "age": 30
            }
        """
        results = await self.list_documents(collection_name, query)
        return results[0] if results else None

    async def insert_one(self, collection_name: str, document: Dict) -> Dict:
        """
        Insert a single document into the collection.
        
        Args:
            collection_name (str): Name of the collection
            document (Dict): Document to insert
            
        Returns:
            Dict: Created document ID
            
        Example:
            >>> await client.insert_one("users", {
            ...     "name": "John",
            ...     "age": 30
            ... })
            {"id": "doc123"}
        """
        return await self.create_document(collection_name, document)
    
    # Collections API
    async def list_collections(self) -> List[CollectionInfo]:
        """
        Get list of all collections.
        
        Headers:
            X-API-Key: <api_key>
            
        Returns:
            List[CollectionInfo]: List of collections with their details
            
        Example:
            >>> await client.list_collections()
            [
                {
                    "name": "users",
                    "description": "User collection",
                    "owner_id": "user123",
                    "created_at": 1647830400
                }
            ]
        """
        return await self._request('GET', '/api/collections')
    
    async def create_collection(self, name: str, description: str = "") -> Dict:
        """
        Create a new collection.
        
        Headers:
            X-API-Key: <api_key>
            Content-Type: application/json
            
        Args:
            name (str): Name of the collection
            description (str, optional): Description of the collection
            
        Returns:
            Dict: Success message
            
        Example:
            >>> await client.create_collection("users", "User collection")
            {"message": "Collection created successfully"}
        """
        data = {
            "name": name,
            "description": description
        }
        return await self._request('POST', '/api/collections', json=data)
    
    async def get_collection(self, name: str) -> Dict:
        """
        Get collection details.
        
        Headers:
            X-API-Key: <api_key>
            
        Args:
            name (str): Name of the collection
            
        Returns:
            Dict: Collection details including document count and size
            
        Example:
            >>> await client.get_collection("users")
            {
                "name": "users",
                "documents_count": 10,
                "size": 1024,
                "indexes": []
            }
        """
        return await self._request('GET', f'/api/collections/{name}')
    
    async def delete_collection(self, name: str) -> Dict:
        """
        Delete a collection and all its documents.
        
        Headers:
            X-API-Key: <api_key>
            
        Args:
            name (str): Name of the collection to delete
            
        Returns:
            Dict: Success message
            
        Example:
            >>> await client.delete_collection("users")
            {"message": "Collection deleted successfully"}
        """
        return await self._request('DELETE', f'/api/collections/{name}')
    
    # Documents API
    async def list_documents(self, collection_name: str, query: Optional[Dict] = None) -> List[DocumentInfo]:
        """
        Get all documents in a collection.
        
        Headers:
            X-API-Key: <api_key>
            
        Args:
            collection_name (str): Name of the collection
            query (Dict, optional): Query operators to filter documents. Supports MongoDB-style operators:
                - Comparison: $eq, $ne, $gt, $gte, $lt, $lte
                - Array: $in, $nin
                - Existence: $exists
                - Type: $type
                - Regex: $regex
                - Logical: $or, $and, $not, $nor
                
        Returns:
            List[DocumentInfo]: List of documents matching the query
            
        Example:
            >>> await client.list_documents("users", {
            ...     "age": { "$gt": 25 },
            ...     "status": { "$in": ["active", "pending"] },
            ...     "$or": [
            ...         { "email": { "$exists": true } },
            ...         { "phone": { "$exists": true } }
            ...     ]
            ... })
        """
        return await self._request('GET', f'/api/{collection_name}', json=query or {})
    
    async def create_document(self, collection_name: str, document: Dict) -> Dict:
        """
        Create a new document in collection.
        
        Headers:
            X-API-Key: <api_key>
            Content-Type: application/json
            
        Args:
            collection_name (str): Name of the collection
            document (Dict): Document data
            
        Returns:
            Dict: Created document ID
            
        Example:
            >>> await client.create_document("users", {
            ...     "name": "John",
            ...     "age": 30,
            ...     "email": "john@example.com"
            ... })
            {"id": "doc123"}
        """
        return await self._request('POST', f'/api/{collection_name}', json=document)
    
    async def get_document(self, collection_name: str, document_id: str, query: Optional[Dict] = None) -> DocumentInfo:
        """
        Get a specific document by ID with optional query operators.
        
        Headers:
            X-API-Key: <api_key>
            
        Args:
            collection_name (str): Name of the collection
            document_id (str): ID of the document to retrieve
            query (Dict, optional): Additional query operators to filter the document. Supports MongoDB-style operators:
                - Comparison: $eq, $ne, $gt, $gte, $lt, $lte
                - Array: $in, $nin
                - Existence: $exists
                - Type: $type
                - Regex: $regex
                - Logical: $or, $and, $not, $nor
                
        Returns:
            DocumentInfo: Document matching the ID and query conditions
            
        Example:
            >>> await client.get_document("users", "123", {
            ...     "age": { "$gt": 25 },
            ...     "status": "active"
            ... })
        """
        return await self._request('GET', f'/api/{collection_name}/{document_id}', json=query or {})
    
    async def update_document(self, collection_name: str, document_id: str, update: Dict) -> Dict:
        """
        Update a document using MongoDB-style update operators or direct field updates.
        
        Headers:
            X-API-Key: <api_key>
            Content-Type: application/json
            
        Args:
            collection_name (str): Name of the collection
            document_id (str): ID of the document to update
            update (Dict): Update operations or direct field updates. Supports operators:
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
                
        Returns:
            Dict: Success message
            
        Example:
            >>> await client.update_document("users", "123", {
            ...     "$set": { "name": "John" },
            ...     "$inc": { "visits": 1 },
            ...     "$push": { "tags": { "$each": ["new", "user"] } },
            ...     "$currentDate": { "lastModified": true }
            ... })
            {"message": "Document updated successfully"}
        """
        return await self._request('PUT', f'/api/{collection_name}/{document_id}', json=update)
    
    async def delete_document(self, collection_name: str, document_id: str) -> Dict:
        """
        Delete a document from a collection.
        
        Headers:
            X-API-Key: <api_key>
            
        Args:
            collection_name (str): Name of the collection
            document_id (str): ID of the document to delete
            
        Returns:
            Dict: Success message
            
        Example:
            >>> await client.delete_document("users", "123")
            {"message": "Document deleted successfully"}
        """
        return await self._request('DELETE', f'/api/{collection_name}/{document_id}')
    
    # Indexes API
    async def create_index(self, collection_name: str, fields: List[str]) -> Dict:
        """
        Create a new index for collection.
        
        Headers:
            X-API-Key: <api_key>
            Content-Type: application/json
            
        Args:
            collection_name (str): Name of the collection
            fields (List[str]): List of fields to index
            
        Returns:
            Dict: Created index information
            
        Example:
            >>> await client.create_index("users", ["email", "age"])
            {
                "message": "Index created",
                "index": {
                    "fields": ["email", "age"],
                    "created_at": 1647830400
                }
            }
        """
        data = {"fields": fields}
        return await self._request('POST', f'/api/collection/{collection_name}/index', json=data)
    
    async def list_indexes(self, collection_name: str) -> Dict:
        """
        Get all indexes for collection.
        
        Headers:
            X-API-Key: <api_key>
            
        Args:
            collection_name (str): Name of the collection
            
        Returns:
            Dict: List of indexes with their details
            
        Example:
            >>> await client.list_indexes("users")
            {
                "indexes": [
                    {
                        "fields": ["email"],
                        "created_at": 1647830400
                    }
                ]
            }
        """
        return await self._request('GET', f'/api/collection/{collection_name}/index')
    
    # Transactions API
    async def start_transaction(self) -> TransactionInfo:
        """
        Start a new transaction.
        
        Headers:
            X-API-Key: <api_key>
            
        Returns:
            TransactionInfo: Transaction details including ID and status
            
        Example:
            >>> await client.start_transaction()
            {
                "transaction_id": "txn123",
                "status": "active"
            }
        """
        return await self._request('POST', '/api/transaction', json={})
    
    async def commit_transaction(self, transaction_id: str) -> Dict:
        """
        Commit a transaction.
        
        Headers:
            X-API-Key: <api_key>
            
        Args:
            transaction_id (str): ID of the transaction to commit
            
        Returns:
            Dict: Transaction status after commit
            
        Example:
            >>> await client.commit_transaction("txn123")
            {"status": "committed"}
        """
        return await self._request('POST', f'/api/transaction/{transaction_id}')
    
    async def rollback_transaction(self, transaction_id: str) -> Dict:
        """
        Rollback a transaction.
        
        Headers:
            X-API-Key: <api_key>
            
        Args:
            transaction_id (str): ID of the transaction to rollback
            
        Returns:
            Dict: Transaction status after rollback
            
        Example:
            >>> await client.rollback_transaction("txn123")
            {"status": "rolled_back"}
        """
        return await self._request('POST', f'/api/transaction/{transaction_id}/rollback')
    
    async def get_transaction_status(self, transaction_id: str) -> TransactionInfo:
        """
        Get transaction status.
        
        Headers:
            X-API-Key: <api_key>
            
        Args:
            transaction_id (str): ID of the transaction
            
        Returns:
            TransactionInfo: Transaction details including status and changes count
            
        Example:
            >>> await client.get_transaction_status("txn123")
            {
                "transaction_id": "txn123",
                "status": "active",
                "start_time": 1647830400,
                "changes_count": 5
            }
        """
        return await self._request('GET', f'/api/transaction/{transaction_id}')

    # Statistics API
    async def get_stats(self) -> DatabaseStats:
        """
        Get database statistics.
        
        Headers:
            X-API-Key: <api_key>
            
        Returns:
            DatabaseStats: Database statistics including collections, documents, and operations
            
        Example:
            >>> await client.get_stats()
            {
                "collections_count": 10,
                "documents_count": 100,
                "data_size": 1024,
                "indexes_count": 5,
                "collections": [
                    {
                        "name": "collection_name",
                        "documents_count": 10,
                        "size": 1024,
                        "indexes": [],
                        "created_at": "timestamp"
                    }
                ],
                "activity": {
                    "labels": ["2024-03-20 10:00", "2024-03-20 11:00"],
                    "data": [5, 10]
                },
                "memory": {
                    "used": 1024,
                    "total": 1073741824
                },
                "operations": {
                    "read": 100,
                    "write": 50,
                    "delete": 10
                }
            }
        """
        return await self._request('GET', '/api/stats')
    
    async def get_detailed_stats(self) -> DetailedStats:
        """
        Get detailed database statistics (admin only).
        
        Headers:
            X-API-Key: <api_key>
            
        Returns:
            DetailedStats: Detailed database statistics including shard and cache information
            
        Example:
            >>> await client.get_detailed_stats()
            {
                "database_info": {
                    "name": "database_name",
                    "num_shards": 3,
                    "num_replicas": 2,
                    "active_transactions": 1
                },
                "shard_stats": {
                    "0": {
                        "documents": 100,
                        "size": 1024
                    }
                },
                "cache_stats": {
                    "0": {
                        "hits": 100,
                        "misses": 10
                    }
                }
            }
        """
        return await self._request('GET', '/api/stats/detailed') 