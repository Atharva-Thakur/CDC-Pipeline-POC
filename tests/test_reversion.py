import sys
import os
import time
import threading
import logging
import psycopg2
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.core.exceptions import ResourceNotFoundError

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from pipeline import CDCPipeline
import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RealReversionTest")

class BackgroundPipeline(CDCPipeline):
    """
    Runs the REAL pipeline logic (pushing to Azure) but in a background thread 
    so the test can interact with the DB.
    """
    def __init__(self):
        super().__init__()
        # Faster flushing for test
        self.BATCH_SIZE = 1
        self.FLUSH_TIMEOUT = 1.0

    def run_background(self):
        stop_event = threading.Event()
        
        def target():
            logger.info("Starting Pipeline Listener (Real Azure Mode)...")
            self.create_replication_slot()
            self.cur.start_replication(slot_name='cdc_slot', decode=True, options={"include-pk": "1"})
            
            import select
            while not stop_event.is_set():
                # Loop similar to main run() but checking stop_event
                timeout = max(0.1, self.FLUSH_TIMEOUT - (time.time() - self.last_flush_time))
                
                if select.select([self.conn], [], [], timeout)[0]:
                    msg = self.cur.read_message()
                    if msg:
                        self.latest_lsn = msg.data_start
                        events = self.parse_wal2json_message(msg.payload)
                        if events:
                            for table, op, data in events:
                                self.process_event(table, op, data)
                        
                        # We don't ack here, we ack in flush_batch
                
                # Check for flush
                if time.time() - self.last_flush_time >= self.FLUSH_TIMEOUT or len(self.batch_actions) >= self.BATCH_SIZE:
                    self.flush_batch()
                    self.cur.send_feedback(reply=True)
            
            logger.info("Pipeline stopped.")

        t = threading.Thread(target=target)
        t.start()
        return t, stop_event

def verify_azure_doc(client, doc_id, expected_exists):
    """
    Polls Azure Search to verify if a document exists or not.
    """
    attempts = 0
    max_attempts = 10
    
    print(f"   [Verify] Checking Azure for ID {doc_id} (Expected Exists: {expected_exists})...")
    
    while attempts < max_attempts:
        try:
            doc = client.get_document(key=doc_id)
            # If we are here, doc exists
            if expected_exists:
                print(f"   [Verify] Found document! {doc}")
                return True
            else:
                # We expected it NOT to exist, but it does. Wait and retry (maybe delete hasn't propagated).
                pass
        except ResourceNotFoundError:
            # Doc does not exist
            if not expected_exists:
                print("   [Verify] Document not found (as expected).")
                return True
            else:
                # We expected it to exist. Wait and retry.
                pass
        
        time.sleep(1.5)
        attempts += 1
        
    return False

def run_reversion_test():
    print("==================================================")
    print("      REAL AZURE REVERSION TEST")
    print("==================================================")
    
    # Setup Azure Verification Client
    search_client = SearchClient(
        endpoint=config.AZURE_SEARCH_ENDPOINT,
        index_name=config.AZURE_SEARCH_INDEX_NAME,
        credential=AzureKeyCredential(config.AZURE_SEARCH_API_KEY)
    )

    # 1. Start Pipeline
    pipeline = BackgroundPipeline()
    thread, stop_event = pipeline.run_background()
    time.sleep(2) # Warmup
    
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # 2. Simulate User Error (Commit Wrong Data)
    email = f"real_mistake_{int(time.time())}@oops.com"
    print(f"\n[Step 1] DB: Insert 'Wrong' Data (Email: {email})")
    cur.execute("INSERT INTO customers (first_name, last_name, email) VALUES ('Wrong', 'One', %s) RETURNING id", (email,))
    mistake_id = str(cur.fetchone()[0])
    
    # Verify it appears in Azure
    if verify_azure_doc(search_client, mistake_id, expected_exists=True):
        print("   [Step 1 Result] PASS: Data successfully synced to Azure.")
    else:
        print("   [Step 1 Result] FAIL: Data never appeared in Azure.")
        stop_event.set()
        thread.join()
        return

    # 3. Simulate Fix (Revert)
    print(f"\n[Step 2] DB: Delete (Revert) row {mistake_id}")
    cur.execute("DELETE FROM customers WHERE id = %s", (mistake_id,))
    
    # Verify it disappears from Azure
    if verify_azure_doc(search_client, mistake_id, expected_exists=False):
        print("   [Step 2 Result] PASS: Data successfully deleted from Azure.")
    else:
        print("   [Step 2 Result] FAIL: Data still exists in Azure after delete.")

    stop_event.set()
    thread.join()
    
    print("\n==================================================")
    print("TEST COMPLETE")

if __name__ == "__main__":
    run_reversion_test()
