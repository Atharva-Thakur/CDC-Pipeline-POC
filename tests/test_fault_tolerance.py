import sys
import os
import time
import threading
import logging
import psycopg2
from faker import Faker

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from pipeline import CDCPipeline
import config

# Configure logging for test
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FaultTest")

# Global flags
SIMULATE_FAILURE = True
TARGET_EMAIL = None

class FailingPipeline(CDCPipeline):
    """
    A pipeline wrapper that simulates a crash/network failure during upload.
    """
    def __init__(self):
        super().__init__()
        # Reduce batch params for faster testing
        self.BATCH_SIZE = 1 
        self.FLUSH_TIMEOUT = 1.0

    def setup_search_client(self):
        # We don't actually need real search for the "failure" part, 
        # but to simulate a crash during the 'try' block of upload.
        # We can just mock the whole flush_batch OR inject a failure in the client.
        # Let's override flush_batch inside to raise exception before Ack.
        pass

    def flush_batch(self):
        # We override to simulate crash
        if not self.batch_actions:
            return
            
        logger.info(f"[FailingPipeline] Attempting to flush {len(self.batch_actions)} items...")
        
        # Check if our target record is in this batch
        found_target = False
        for table, op, doc in self.batch_actions:
            if table == 'customers' and doc.get('email') == TARGET_EMAIL:
                found_target = True
                break
        
        if found_target and SIMULATE_FAILURE:
            logger.error("[FailingPipeline] !!! SIMULATING SYSTEM CRASH / NETWORK FAILURE !!!")
            logger.error("[FailingPipeline] Exiting without acknowledging LSN to Postgres.")
            # We explicitly exit the process or raise an exception that stops the run loop
            # Raising SystemExit acts like a crash
            raise SystemExit("Simulated Crash")
            
        # If we didn't crash, behave normally (mock success)
        # We fake the ack here just to clear the slot if needed, 
        # but for this test we only care about the crash case.
        self.batch_actions = []
        if self.latest_lsn:
             self.cur.send_feedback(flush_lsn=self.latest_lsn)

class RecoveryPipeline(CDCPipeline):
    """
    A pipeline that behaves normally and verifies it received the message.
    """
    def __init__(self):
        super().__init__()
        self.received_target = False
        self.BATCH_SIZE = 1
        self.FLUSH_TIMEOUT = 1.0

    def process_event(self, table, op, data):
        # Check if we got the target
        if table == 'customers' and data.get('email') == TARGET_EMAIL:
            logger.info("[RecoveryPipeline] SUCCESS! Received the target record again.")
            self.received_target = True
            
        super().process_event(table, op, data)

    def run_check(self, duration=10):
        # Runs for a short duration to check for messages
        logger.info("[RecoveryPipeline] Starting recovery check...")
        self.create_replication_slot()
        self.cur.start_replication(slot_name='cdc_slot', decode=True, options={"include-pk": "1"})
        
        start = time.time()
        while time.time() - start < duration:
            try:
                msg = self.cur.read_message()
                if msg:
                    payload = msg.payload
                    events = self.parse_wal2json_message(payload)
                    if events:
                        for table, op, data in events:
                            self.process_event(table, op, data)
                    
                    # Ack progress
                    self.cur.send_feedback(flush_lsn=msg.data_start)
                    
                    if self.received_target:
                        return True
            except Exception as e:
                pass
            time.sleep(0.1)
            self.cur.send_feedback(reply=True)
            
        return False

def insert_test_record():
    global TARGET_EMAIL
    fake = Faker()
    f_name = "FaultTest"
    l_name = "User"
    TARGET_EMAIL = f"fault.test.{fake.uuid4()}@example.com"
    
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    logger.info(f"Inserting test record: {TARGET_EMAIL}")
    cur.execute(
        "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s)",
        (f_name, l_name, TARGET_EMAIL)
    )
    cur.close()
    conn.close()

def run_experiment():
    print("==================================================")
    print("      FAULT TOLERANCE / ROLLBACK TEST")
    print("==================================================")
    
    # 1. Insert Record
    insert_test_record()
    
    # 2. Run Failing Pipeline
    print("\n>>> STEP 1: Running 'FailingPipeline'...")
    print("    Expected behavior: It should detect the record, try to upload, simulated crash, and EXIT.")
    
    failing_pipe = FailingPipeline()
    try:
        failing_pipe.run()
    except SystemExit:
        print("    [Pass] Pipeline crashed as expected.")
        print("    [Cleanup] Closing crashed connection to release replication slot check...")
        if hasattr(failing_pipe, 'conn'):
            failing_pipe.conn.close()
        # Give Postgres a moment to realize the client is gone and free the slot
        time.sleep(2)
    except Exception as e:
        print(f"    [Fail] Unexpected exception: {e}")

    # 3. Run Recovery Pipeline
    print("\n>>> STEP 2: Running 'RecoveryPipeline'...")
    print("    Expected behavior: Postgres should RESEND the unacknowledged transaction.")
    
    recovery_pipe = RecoveryPipeline()
    success = recovery_pipe.run_check(duration=10)
    
    print("\n==================================================")
    if success:
        print("RESULT: PASSED. The pipeline successfully recovered the lost message.")
        print("This proves that if Azure upload fails, Postgres retains the data (Rollback behavior).")
    else:
        print("RESULT: FAILED. The message was not received again. Data loss occurred.")
    print("==================================================")

if __name__ == "__main__":
    run_experiment()
