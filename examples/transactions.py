"""
Transaction examples for MaseDB client.

This example demonstrates:
- Starting and managing transactions
- Performing multiple operations within a transaction
- Handling transaction rollbacks
- Error handling in transactions
- Transaction status monitoring
"""

from masedb import MaseDBClient
from masedb.exceptions import MaseDBError

def main():
    client = MaseDBClient(api_key="your_api_key")
    
    try:
        # Create collections
        client.create_collection("accounts", "Bank accounts collection")
        client.create_collection("transactions", "Transaction history collection")
        print("Created collections")
        
        # Insert initial account data
        account1 = {
            "account_id": "ACC001",
            "owner": "John Doe",
            "balance": 1000.00,
            "currency": "USD",
            "status": "active"
        }
        
        account2 = {
            "account_id": "ACC002",
            "owner": "Jane Smith",
            "balance": 500.00,
            "currency": "USD",
            "status": "active"
        }
        
        client.insert_one("accounts", account1)
        client.insert_one("accounts", account2)
        print("Created initial accounts")
        
        # Example 1: Successful money transfer
        print("\nExample 1: Successful money transfer")
        try:
            # Start transaction
            transaction = client.start_transaction()
            print(f"Started transaction: {transaction['transaction_id']}")
            
            # Perform transfer
            amount = 200.00
            
            # Update source account
            client.update_document("accounts", "ACC001", {
                "$inc": {"balance": -amount}
            })
            
            # Update destination account
            client.update_document("accounts", "ACC002", {
                "$inc": {"balance": amount}
            })
            
            # Record transaction
            transfer_record = {
                "transaction_id": transaction["transaction_id"],
                "type": "transfer",
                "from_account": "ACC001",
                "to_account": "ACC002",
                "amount": amount,
                "currency": "USD",
                "status": "completed"
            }
            client.insert_one("transactions", transfer_record)
            
            # Commit transaction
            client.commit_transaction(transaction["transaction_id"])
            print("Transfer completed successfully")
            
            # Verify balances
            acc1 = client.find_one("accounts", {"account_id": "ACC001"})
            acc2 = client.find_one("accounts", {"account_id": "ACC002"})
            print(f"New balances - Account 1: ${acc1['balance']}, Account 2: ${acc2['balance']}")
            
        except MaseDBError as e:
            print(f"Transfer failed: {e.message}")
            # Transaction will be automatically rolled back
            
        # Example 2: Failed transfer (insufficient funds)
        print("\nExample 2: Failed transfer (insufficient funds)")
        try:
            # Start transaction
            transaction = client.start_transaction()
            print(f"Started transaction: {transaction['transaction_id']}")
            
            # Attempt transfer
            amount = 2000.00  # More than available balance
            
            # Update source account
            client.update_document("accounts", "ACC001", {
                "$inc": {"balance": -amount}
            })
            
            # Update destination account
            client.update_document("accounts", "ACC002", {
                "$inc": {"balance": amount}
            })
            
            # Record transaction
            transfer_record = {
                "transaction_id": transaction["transaction_id"],
                "type": "transfer",
                "from_account": "ACC001",
                "to_account": "ACC002",
                "amount": amount,
                "currency": "USD",
                "status": "failed"
            }
            client.insert_one("transactions", transfer_record)
            
            # Commit transaction
            client.commit_transaction(transaction["transaction_id"])
            
        except MaseDBError as e:
            print(f"Transfer failed as expected: {e.message}")
            # Rollback transaction
            client.rollback_transaction(transaction["transaction_id"])
            print("Transaction rolled back")
            
            # Verify balances remain unchanged
            acc1 = client.find_one("accounts", {"account_id": "ACC001"})
            acc2 = client.find_one("accounts", {"account_id": "ACC002"})
            print(f"Balances unchanged - Account 1: ${acc1['balance']}, Account 2: ${acc2['balance']}")
            
        # Example 3: Transaction status monitoring
        print("\nExample 3: Transaction status monitoring")
        try:
            # Start transaction
            transaction = client.start_transaction()
            print(f"Started transaction: {transaction['transaction_id']}")
            
            # Check initial status
            status = client.get_transaction_status(transaction["transaction_id"])
            print(f"Initial status: {status['status']}")
            
            # Perform some operations
            client.update_document("accounts", "ACC001", {
                "$inc": {"balance": 100}
            })
            
            # Check status after operation
            status = client.get_transaction_status(transaction["transaction_id"])
            print(f"Status after operation: {status['status']}")
            print(f"Changes count: {status['changes_count']}")
            
            # Commit transaction
            client.commit_transaction(transaction["transaction_id"])
            
            # Check final status
            status = client.get_transaction_status(transaction["transaction_id"])
            print(f"Final status: {status['status']}")
            
        except MaseDBError as e:
            print(f"Error: {e.message}")
            
    except MaseDBError as e:
        print(f"Error: {e.message}")
        if e.code:
            print(f"Error code: {e.code}")
        if e.details:
            print(f"Error details: {e.details}")
    finally:
        # Clean up
        try:
            client.delete_collection("accounts")
            client.delete_collection("transactions")
            print("\nCollections deleted")
        except MaseDBError:
            pass

if __name__ == "__main__":
    main() 