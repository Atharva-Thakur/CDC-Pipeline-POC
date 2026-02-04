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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RollbackTest")

class SilentPipeline(CDCPipeline):
    """
    Stops running after a fixed timeout and captures events.
    """
    def __init__(self):
        self.captured_events = []
        super().__init__()
        self.BATCH_SIZE = 1
        self.FLUSH_TIMEOUT = 0.5
    
    def setup_search_client(self):
        # Mock search client
        self.search_client = MagicMock()
        self.search_client.merge_or_upload_documents.return_value = []
        
    def process_event(self, table, op, data):
        logger.info(f"CAPTURED EVENT: {op} on {table}")
        self.captured_events.append((table, op, data))
        super().process_event(table, op, data)

    def run_for_duration(self, duration=10):
        stop_event = threading.Event()
        
        def target():
            logger.info("Starting listener...")
            self.create_replication_slot()
            self.cur.start_replication(slot_name='cdc_slot', decode=True, options={"include-pk": "1"})
            
            start = time.time()
            import select
            while time.time() - start < duration:
                timeout = 0.5
                if select.select([self.conn], [], [], timeout)[0]:
                    msg = self.cur.read_message()
                    if msg:
                        self.latest_lsn = msg.data_start
                        events = self.parse_wal2json_message(msg.payload)
                        if events:
                            for table, op, data in events:
                                # Start of Critical Section
                                # If we see an event here, it means it leaked!
                                self.process_event(table, op, data)
                        
                        self.cur.send_feedback(flush_lsn=msg.data_start)
                        
                # Keep alive
                if time.time() - self.last_flush_time > 2.0:
                    self.cur.send_feedback(reply=True)
                    self.last_flush_time = time.time()
        
        t = threading.Thread(target=target)
        t.start()
        return t

def run_rollback_test():
    print("==================================================")
    print("      ROLLBACK ISOLATION TEST")
    print("==================================================")
    print("Scenario: Start a transaction, insert data, then ROLLBACK.")
    print("Expected: The pipeline should NEVER see this data.")
    
    # 1. Start Pipeline
    pipeline = SilentPipeline()
    t = pipeline.run_for_duration(duration=8)
    time.sleep(2) # Wait for startup
    
    # 2. Perform Rollback Transaction
    print("\n[DB] Starting Transaction (BEGIN)...")
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )
    # conn.autocommit = False means a transaction is started automatically
    conn.autocommit = False 
    cur = conn.cursor()
    
    email = "ghost.user@example.com"
    print(f"[DB] Inserting '{email}' (Uncommitted)...")
    cur.execute("INSERT INTO customers (first_name, last_name, email) VALUES ('Ghost', 'Rider', %s)", (email,))
    
    print("[DB] Sleeping 3 seconds (Transaction Open)...")
    time.sleep(3)
    
    print("[DB] Executing ROLLBACK...")
    conn.rollback()
    print("[DB] Transaction Rolled Back.")
    
    conn.close()
    
    # Wait for pipeline to finish
    t.join()
    
    # 3. Analyze Results
    print("\n[Analysis] Checking captured events...")
    ghost_events = [e for e in pipeline.captured_events if e[2].get('email') == email]
    
    if len(ghost_events) == 0:
        print("RESULT: PASSED. Zero events captured for the rolled-back transaction.")
        print("This confirms that CDC only streams COMMITTED data.")
    else:
        print(f"RESULT: FAILED. The pipeline saw uncommitted data! Found: {ghost_events}")

if __name__ == "__main__":
    try:
        run_rollback_test()
    except Exception as e:
        print(f"Test crashed: {e}")
