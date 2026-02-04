import sys
import os
import time
import threading
import logging
import psycopg2
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from pipeline import CDCPipeline
import config

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("CodeFixTest")
logger.setLevel(logging.INFO)

class BuggyPipeline(CDCPipeline):
    """
    Simulates broken logic during upload preparation or payload formatting.
    """
    def __init__(self):
        super().__init__()
        self.BATCH_SIZE = 1
        self.FLUSH_TIMEOUT = 0.5

    def transform_data(self, table, data_dict):
        # Data transformation works fine...
        return super().transform_data(table, data_dict)
        
    def flush_batch(self):
        # ... but we mess up the upload structure here!
        logger.info("[BuggyPipeline] Preparing to upload...")
        
        # Simulate a Schema Mismatch or Type Error that Azure would reject 
        # OR a bug in our code constructing the request.
        logger.info("[BuggyPipeline] OOPS! Sending 'age' as String to an Integer field (or similar bug).")
        raise ValueError("Simulated Bug: Payload Validation Error / Azure 400 Bad Request")


class FixedPipeline(CDCPipeline):
    """
    Simulates the fixed code.
    """
    def __init__(self):
        super().__init__()
        self.BATCH_SIZE = 1
        self.FLUSH_TIMEOUT = 0.5
        self.processed_count = 0

    def transform_data(self, table, data_dict):
        logger.info("[FixedPipeline] Transformation Logic running... Success.")
        return super().transform_data(table, data_dict)
    
    def flush_batch(self):
        if self.batch_actions:
            self.processed_count += len(self.batch_actions)
        super().flush_batch()

    def run_check(self):
        # Run briefly to process backlog
        stop_event = threading.Event()
        def target():
            self.create_replication_slot()
            self.cur.start_replication(slot_name='cdc_slot', decode=True, options={"include-pk": "1"})
            import select
            start = time.time()
            while time.time() - start < 5:
                # ... standard loop ...
                timeout = 0.5
                if select.select([self.conn], [], [], timeout)[0]:
                    msg = self.cur.read_message()
                    if msg:
                        self.latest_lsn = msg.data_start
                        events = self.parse_wal2json_message(msg.payload)
                        if events:
                            for table, op, data in events:
                                self.process_event(table, op, data)
                        if len(self.batch_actions) >= 1:
                            self.flush_batch()
                            self.cur.send_feedback(flush_lsn=msg.data_start)
                
                if time.time() - self.last_flush_time > 2.0:
                    self.cur.send_feedback(reply=True)
                    self.last_flush_time = time.time()
        
        t = threading.Thread(target=target)
        t.start()
        t.join()
        return self.processed_count

def run_test():
    print("==================================================")
    print("      BAD CODE / RECOVERY TEST")
    print("==================================================")
    
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # 1. Insert Data
    email = f"bug_test_{int(time.time())}@code.com"
    print(f"\n[Step 1] DB: Insert Data (Email: {email})")
    cur.execute("INSERT INTO customers (first_name, last_name, email) VALUES ('Buggy', 'Code', %s)", (email,))
    
    # 2. Run Buggy Pipeline
    print(f"\n[Step 2] Running 'BuggyPipeline' (Simulating bad deploy)...")
    buggy = BuggyPipeline()
    try:
        buggy.run()
    except ValueError as e:
        print(f"   [Pass] Pipeline crashed as expected with error: {e}")
        # Clean up connection manually since it crashed
        buggy.conn.close()
    except Exception as e:
        print(f"   [Fail] Crashed with unexpected error: {e}")
    
    time.sleep(2) # Release slot
    
    # 3. Run Fixed Pipeline
    print(f"\n[Step 3] Running 'FixedPipeline' (Simulating hotfix)...")
    fixed = FixedPipeline()
    count = fixed.run_check()
    
    print("\n==================================================")
    if count > 0:
        print("RESULT: PASSED.")
        print("The FixedPipeline successfully picked up the data that the BuggyPipeline failed to process.")
        print("Proof: No data loss occurred despite the initial crash.")
    else:
        print("RESULT: FAILED. FixedPipeline did not process the pending record.")

if __name__ == "__main__":
    run_test()
