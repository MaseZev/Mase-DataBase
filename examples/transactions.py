"""
Transactions example for MaseDB Python Client.

This example demonstrates:
- Starting and managing transactions
- Performing multiple operations within a transaction
- Handling transaction rollbacks
- Error handling in transactions
- Transaction status monitoring
"""

from masedb import MaseDBClient
from masedb.exceptions import MaseDBError, BadRequestError, UnauthorizedError
from datetime import datetime

def main():
    # Initialize client with your API key
    client = MaseDBClient(api_key="your_api_key")
    
    try:
        # Create collections
        print("\n1. Creating collections...")
        client.create_collection("accounts", "Collection for bank accounts")
        client.create_collection("transactions", "Collection for transaction history")
        print("Collections created successfully")
        
        # Create initial accounts
        print("\n2. Creating initial accounts...")
        accounts = [
            {
                "account_id": "ACC001",
                "owner": "John Doe",
                "balance": 1000.00,
                "currency": "USD",
                "status": "active",
                "created_at": datetime.utcnow().isoformat()
            },
            {
                "account_id": "ACC002",
                "owner": "Jane Smith",
                "balance": 500.00,
                "currency": "USD",
                "status": "active",
                "created_at": datetime.utcnow().isoformat()
            }
        ]
        
        for account in accounts:
            result = client.create_document("accounts", account)
            print(f"Account created: {result}")
        
        # Example 1: Simple money transfer
        print("\nExample 1: Simple money transfer")
        try:
            # Start transaction
            transaction = client.start_transaction()
            transaction_id = transaction["transaction_id"]
            print(f"Started transaction: {transaction_id}")
            
            # Perform transfer
            amount = 100.00
            
            # Update sender's account
            client.update_document("accounts", "ACC001", {
                "$inc": {"balance": -amount},
                "$currentDate": {"last_modified": True}
            })
            
            # Update receiver's account
            client.update_document("accounts", "ACC002", {
                "$inc": {"balance": amount},
                "$currentDate": {"last_modified": True}
            })
            
            # Record transaction
            client.create_document("transactions", {
                "transaction_id": transaction_id,
                "from_account": "ACC001",
                "to_account": "ACC002",
                "amount": amount,
                "currency": "USD",
                "status": "completed",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Commit transaction
            client.commit_transaction(transaction_id)
            print("Transaction committed successfully")
            
        except Exception as e:
            print(f"Error during transfer: {e}")
            client.rollback_transaction(transaction_id)
            print("Transaction rolled back")
        
        # Example 2: Complex transaction with multiple operations
        print("\nExample 2: Complex transaction with multiple operations")
        try:
            # Start transaction
            transaction = client.start_transaction()
            transaction_id = transaction["transaction_id"]
            print(f"Started transaction: {transaction_id}")
            
            # Create new account
            new_account = {
                "account_id": "ACC003",
                "owner": "Bob Johnson",
                "balance": 0.00,
                "currency": "USD",
                "status": "pending",
                "created_at": datetime.utcnow().isoformat()
            }
            client.create_document("accounts", new_account)
            
            # Transfer money from multiple accounts
            transfers = [
                {"from": "ACC001", "to": "ACC003", "amount": 50.00},
                {"from": "ACC002", "to": "ACC003", "amount": 25.00}
            ]
            
            for transfer in transfers:
                # Update sender's account
                client.update_document("accounts", transfer["from"], {
                    "$inc": {"balance": -transfer["amount"]},
                    "$currentDate": {"last_modified": True}
                })
                
                # Update receiver's account
                client.update_document("accounts", transfer["to"], {
                    "$inc": {"balance": transfer["amount"]},
                    "$currentDate": {"last_modified": True}
                })
                
                # Record transaction
                client.create_document("transactions", {
                    "transaction_id": transaction_id,
                    "from_account": transfer["from"],
                    "to_account": transfer["to"],
                    "amount": transfer["amount"],
                    "currency": "USD",
                    "status": "completed",
                    "timestamp": datetime.utcnow().isoformat()
                })
            
            # Update new account status
            client.update_document("accounts", "ACC003", {
                "$set": {"status": "active"},
                "$currentDate": {"last_modified": True}
            })
            
            # Commit transaction
            client.commit_transaction(transaction_id)
            print("Complex transaction committed successfully")
            
        except Exception as e:
            print(f"Error during complex transaction: {e}")
            client.rollback_transaction(transaction_id)
            print("Transaction rolled back")
        
        # Example 3: Transaction status monitoring
        print("\nExample 3: Transaction status monitoring")
        try:
            # Start transaction
            transaction = client.start_transaction()
            transaction_id = transaction["transaction_id"]
            print(f"Started transaction: {transaction_id}")
            
            # Check initial status
            status = client.get_transaction_status(transaction_id)
            print(f"Initial status: {status}")
            
            # Perform some operations
            client.update_document("accounts", "ACC001", {
                "$inc": {"balance": 100.00},
                "$currentDate": {"last_modified": True}
            })
            
            # Check status after operations
            status = client.get_transaction_status(transaction_id)
            print(f"Status after operations: {status}")
            
            # Commit transaction
            client.commit_transaction(transaction_id)
            
            # Check final status
            status = client.get_transaction_status(transaction_id)
            print(f"Final status: {status}")
            
        except Exception as e:
            print(f"Error during status monitoring: {e}")
            client.rollback_transaction(transaction_id)
            print("Transaction rolled back")
        
        # Clean up
        print("\n4. Cleaning up...")
        client.delete_collection("accounts")
        client.delete_collection("transactions")
        print("Collections deleted")
        
    except BadRequestError as e:
        print(f"Invalid request: {e}")
    except UnauthorizedError as e:
        print(f"Authentication failed: {e}")
    except MaseDBError as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    main() 