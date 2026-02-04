import sys
import os
import time
import threading
import logging
import psycopg2
from faker import Faker
from unittest.mock import MagicMock

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from pipeline import CDCPipeline
import config

# Configure logging
logging.basicConfig(level=logging.ERROR) # Keep logs quiet for the test output
logger = logging.getLogger("ScenarioTest")
logger.setLevel(logging.INFO)

class TestPipeline(CDCPipeline):
    """
    A wrapper around CDCPipeline to capture actions instead of sending to Azure.
    """
    def __init__(self):
        self.captured_actions = []
        super().__init__()
        # Faster batching for tests
        self.BATCH_SIZE = 1
        self.FLUSH_TIMEOUT = 1.0

    def setup_search_client(self):
        # Mock the search client so we don't actually hit Azure
        self.search_client = MagicMock()
        self.search_client.merge_or_upload_documents.return_value = [MagicMock(succeeded=True)]
        self.search_client.delete_documents.return_value = [MagicMock(succeeded=True)]

    def flush_batch(self):
        # Capture what is about to be flushed
        if self.batch_actions:
            self.captured_actions.extend(self.batch_actions)
        
        # Call original to handle the logic/acking (it will use the mock client)
        super().flush_batch()

    def run_with_timeout(self, timeout=15):
        """
        Runs the pipeline in a separate thread for a fixed duration.
        """
        stop_event = threading.Event()
        
        def target():
            logger.info("Starting TestPipeline...")
            self.create_replication_slot()
            self.cur.start_replication(slot_name='cdc_slot', decode=True, options={"include-pk": "1"})

            start_time = time.time()
            import select
            
            while not stop_event.is_set():
                if time.time() - start_time > timeout:
                    break

                # Same logic as main pipeline loop
                wait_t = max(0.1, self.FLUSH_TIMEOUT - (time.time() - self.last_flush_time))
                if select.select([self.conn], [], [], wait_t)[0]:
                    msg = self.cur.read_message()
                    if msg:
                        # Log raw payload for debugging
                        # print(f"DEBUG: {msg.payload}")
                        self.latest_lsn = msg.data_start
                        events = self.parse_wal2json_message(msg.payload)
                        if events:
                            for table, op, data in events:
                                self.process_event(table, op, data)
                
                # Check flush
                if time.time() - self.last_flush_time >= self.FLUSH_TIMEOUT or len(self.batch_actions) >= self.BATCH_SIZE:
                    self.flush_batch()
                    self.cur.send_feedback(reply=True)
            
            logger.info("TestPipeline stopped.")

        t = threading.Thread(target=target)
        t.start()
        return t

def db_execute(query, params=None):
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(query, params)
    if cur.description:
        res = cur.fetchall()
    else:
        res = None
    cur.close()
    conn.close()
    return res

def test_update_workflow():
    print("\n>>> TEST SCENARIO 1: Insert -> Update -> Delete")
    pipeline = TestPipeline()
    thread = pipeline.run_with_timeout(timeout=12)
    
    fake = Faker()
    email = f"test_scn_1_{fake.uuid4()}@example.com"
    
    # 1. INSERT
    print(f"    Action: Inserting user {email}")
    res = db_execute("INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id", 
               ('Test', 'User', email))
    user_id = res[0][0]
    time.sleep(3) # Wait for pipeline
    
    # 2. UPDATE
    print(f"    Action: Updating user {user_id} name to 'Updated'")
    db_execute("UPDATE customers SET first_name = 'Updated' WHERE id = %s", (user_id,))
    time.sleep(3)

    # 3. DELETE
    print(f"    Action: Deleting user {user_id}")
    db_execute("DELETE FROM customers WHERE id = %s", (user_id,))
    time.sleep(3)
    
    thread.join()
    
    # Verify
    actions = pipeline.captured_actions
    # Filter for our user_id
    my_actions = [a for a in actions if a[2].get('id') == str(user_id)]
    
    print(f"    Captured {len(my_actions)} events for ID {user_id}")
    
    # Check sequences
    ops = [a[1] for a in my_actions]
    print(f"    Operations sequence: {ops}")
    
    if ops == ['INSERT', 'UPDATE', 'DELETE']:
        print("    [PASS] Sequence matched: INSERT -> UPDATE -> DELETE")
    else:
        print("    [FAIL] Sequence mismatch.")

def test_address_merge():
    print("\n>>> TEST SCENARIO 2: Customer + Address Merge")
    pipeline = TestPipeline()
    thread = pipeline.run_with_timeout(timeout=10)
    
    fake = Faker()
    email = f"test_scn_2_{fake.uuid4()}@example.com"
    
    # 1. INSERT Customer
    print(f"    Action: Inserting customer")
    res = db_execute("INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id", 
               ('Merge', 'Tester', email))
    c_id = res[0][0]
    time.sleep(3)
    
    # 2. INSERT Address
    print(f"    Action: Inserting address for customer {c_id}")
    db_execute("INSERT INTO addresses (customer_id, street, city, zip_code) VALUES (%s, %s, %s, %s)",
               (c_id, '123 Cloud St', 'Azure City', '99999'))
    time.sleep(3)
    
    thread.join()
    
    actions = pipeline.captured_actions
    my_actions = [a for a in actions if a[2].get('id') == str(c_id)]
    
    print(f"    Captured {len(my_actions)} events for ID {c_id}")
    
    # Check content
    customer_evt = next((a[2] for a in my_actions if a[0] == 'customers'), {})
    address_evt = next((a[2] for a in my_actions if a[0] == 'addresses'), {})
    
    has_name = customer_evt.get('full_name') == 'Merge Tester'
    has_street = address_evt.get('street') == '123 Cloud St'
    
    if has_name and has_street:
         print("    [PASS] Captured independent events for both Customer and Address.")
         print(f"           - Customer Event: {customer_evt}")
         print(f"           - Address Event: {address_evt}")
    else:
         print(f"    [FAIL] Missing data. Name found: {has_name}, Street found: {has_street}")
         print(f"           - All Actions: {my_actions}")

if __name__ == "__main__":
    test_update_workflow()
    test_address_merge()
