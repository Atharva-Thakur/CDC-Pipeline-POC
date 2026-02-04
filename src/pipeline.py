import time
import json
import datetime
import re
import select
import psycopg2
import psycopg2.extras
import logging
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

import config

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(config.LOG_FILE_PATH)
    ]
)
logger = logging.getLogger("CDC_Pipeline")

class CDCPipeline:
    def __init__(self):
        self.connect_db()
        self.setup_search_client()
        self.batch_actions = []
        self.last_flush_time = time.time()
        self.latest_lsn = None
        self.BATCH_SIZE = 50
        self.FLUSH_TIMEOUT = 5.0  # seconds

    def connect_db(self):
        logger.info("Connecting to PostgreSQL database...")
        self.conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            connection_factory=psycopg2.extras.LogicalReplicationConnection
        )
        self.cur = self.conn.cursor()

    def setup_search_client(self):
        if config.AZURE_SEARCH_ENDPOINT and config.AZURE_SEARCH_API_KEY:
            self.search_client = SearchClient(
                endpoint=config.AZURE_SEARCH_ENDPOINT,
                index_name=config.AZURE_SEARCH_INDEX_NAME,
                credential=AzureKeyCredential(config.AZURE_SEARCH_API_KEY)
            )
            logger.info(f"Connected to Azure AI Search Index: {config.AZURE_SEARCH_INDEX_NAME}")
        else:
            self.search_client = None
            logger.warning("Azure Search client not initialized (missing credentials).")

    def create_replication_slot(self, slot_name='cdc_slot'):
        try:
            self.cur.create_replication_slot(slot_name, output_plugin='wal2json')
            logger.info(f"Replication slot '{slot_name}' created successfully.")
        except psycopg2.errors.DuplicateObject:
            logger.info(f"Replication slot '{slot_name}' already exists. Reusing it.")
        except Exception as e:
            logger.error(f"Error creating slot: {e}")

    def transform_data(self, table, data_dict):
        """
        Apply transformations based on table source.
        Returns a dict partial document for Azure Search.
        """
        transformed = {}
        
        if table == 'customers':
            # Map Customer Primary Key to Search Index ID
            if 'id' in data_dict:
                transformed['id'] = str(data_dict['id'])
            
            # Customer fields
            first = data_dict.get('first_name', '').strip("'")
            last = data_dict.get('last_name', '').strip("'")
            if first or last:
                transformed['full_name'] = f"{first} {last}".strip()
            
            if 'email' in data_dict:
                transformed['email'] = data_dict['email'].strip("'")
                
            transformed['last_updated_by'] = 'customers_table'
            
        elif table == 'addresses':
            # Map Address Foreign Key (customer_id) to Search Index ID
            # This allows merging addres info into the SAME document
            if 'customer_id' in data_dict:
                transformed['id'] = str(data_dict['customer_id'])
            
            # Address fields
            if 'street' in data_dict:
                transformed['street'] = data_dict['street'].strip("'")
            if 'city' in data_dict:
                transformed['city'] = data_dict['city'].strip("'")
            if 'zip_code' in data_dict:
                transformed['zip_code'] = data_dict['zip_code'].strip("'")
                
            transformed['last_updated_by'] = 'addresses_table'

        transformed['processed_at'] = datetime.datetime.utcnow().isoformat() + "Z"
        
        return transformed

    def parse_wal2json_message(self, payload):
        """
        Parses 'wal2json' plugin output.
        Expects JSON payload with 'change' list.
        """
        try:
            data_events = []
            message = json.loads(payload)
            
            if 'change' not in message:
                return []

            for change in message['change']:
                table = change.get('table')
                if table not in ['customers', 'addresses'] or change.get('schema') != 'public':
                    continue

                op = None
                kind = change.get('kind')
                if kind == 'insert':
                    op = "INSERT"
                elif kind == 'update':
                    op = "UPDATE"
                elif kind == 'delete':
                    op = "DELETE"
                
                if not op:
                    continue

                # Map columnnames and columnvalues to dictionary
                col_names = change.get('columnnames', [])
                col_values = change.get('columnvalues', [])
                data = dict(zip(col_names, col_values))
                
                # Handling Deletes (getting IDs)
                if op == "DELETE" and not data:
                     old_keys = change.get('oldkeys', {})
                     if old_keys:
                         k_names = old_keys.get('keynames', [])
                         k_values = old_keys.get('keyvalues', [])
                         data = dict(zip(k_names, k_values))

                data_events.append((table, op, data))

            return data_events

        except Exception as e:
            logger.error(f"Error parsing payload: {e}")
            return []

    def flush_batch(self):
        logger.info(f"----- [BATCH FLUSH] -----")
        
        # 1. Upload to Azure Search if there are pending actions
        if self.batch_actions:
            logger.info(f"Flushing batch of {len(self.batch_actions)} events...")
            to_delete = []
            to_merge = []
            
            for table, op, doc in self.batch_actions:
                # We interpret DELETE as deleting the whole document if it comes from the 'customers' table.
                # If a delete comes from 'addresses', we might just want to blank out fields, 
                # but for this POC we'll only delete the full doc if the customer is deleted.
                
                if op == "DELETE" and table == 'customers' and 'id' in doc:
                    to_delete.append({"id": doc['id']})
                else:
                    # In Azure Search, if you 'merge' a document with just {"id": "1", "street": "Main"}
                    # and the document {"id": "1", "name": "Bob"} exists, result is {"id": "1", "name": "Bob", "street": "Main"}
                    # If it doesn't exist, it creates {"id": "1", "street": "Main"} (which waits for name)
                    if 'id' in doc:
                        to_merge.append(doc)
            
            start_time = time.time()
            try:
                if self.search_client:
                    success_flag = True
                    if to_delete:
                        logger.info(f"Deleting {len(to_delete)} documents...")
                        # In production, handle individual errors
                        self.search_client.delete_documents(documents=to_delete)
                    
                    if to_merge:
                        logger.info(f"Merging/Uploading {len(to_merge)} documents...")
                        results = self.search_client.merge_or_upload_documents(documents=to_merge)
                        if not results or not all(r.succeeded for r in results):
                            success_flag = False
                            failures = [r for r in results if not r.succeeded]
                            logger.error(f"Azure Search Output: {len(failures)} FAILURES.")
                    
                    if not success_flag:
                         raise Exception("Azure Search reported partial or full failure in batch. Triggering retry.")

                    logger.info("Azure Search Output: ALL SUCCESS.")
                else:
                    logger.warning("Search client not configured, skipping push.")
            except Exception as e:
                logger.error(f"Error pushing batch to search: {e}")
                logger.info("Retrying in 5 seconds...")
                time.sleep(5)
                # If upload fails, DO NOT acknowledge LSN, so we retry on restart
                return
            finally:
                elapsed_time = time.time() - start_time
                logger.info(f"Batch processing time: {elapsed_time:.4f} seconds")

        # 2. Acknowledge LSN to Postgres (Commit Progress)
        # We do this only after successful upload (or if batch was empty/filtered)
        if self.latest_lsn:
            logger.info(f"Sending feedback to Postgres: flushed up to LSN {self.latest_lsn}")
            self.cur.send_feedback(flush_lsn=self.latest_lsn)

        # Reset batch
        self.batch_actions = []
        self.last_flush_time = time.time()

    def process_event(self, table, op, data):
        """
        Buffer events instead of pushing immediately.
        """
        doc = self.transform_data(table, data)
        self.batch_actions.append((table, op, doc))
        
        # We trigger flush in the loop now to handle LSN updates correctly

    def run(self):
        logger.info("Starting CDC Pipeline Service...")
        self.create_replication_slot()
        
        logger.info("Listening for WAL Stream changes on 'cdc_slot'...")
        self.cur.start_replication(slot_name='cdc_slot', decode=True, options={"include-pk": "1"})

        while True:
            # check functionality of select
            # If nothing happens for FLUSH_TIMEOUT, we might want to wake up to flush
            
            timeout = max(0.1, self.FLUSH_TIMEOUT - (time.time() - self.last_flush_time))
            
            # Non-blocking check for data
            if select.select([self.conn], [], [], timeout)[0]:
                msg = self.cur.read_message()
                if msg:
                     payload = msg.payload
                     logger.info(f"Raw WAL Payload: {payload}")
                     self.latest_lsn = msg.data_start
                     
                     # Try to parse wal2json message
                     events = self.parse_wal2json_message(payload)
            
                     if events:
                        logger.info(f"Received {len(events)} events in WAL message")

                        for table, op, data in events:
                            self.process_event(table, op, data)
                
                     # We DO NOT acknowledge here immediately. 
                     # Feedback is sent in flush_batch to ensure at-least-once delivery.
            
            # Check for flush timeout
            if time.time() - self.last_flush_time >= self.FLUSH_TIMEOUT or len(self.batch_actions) >= self.BATCH_SIZE:
                 self.flush_batch()
                 # Send keepalive (heartbeat)
                 self.cur.send_feedback(reply=True)

if __name__ == "__main__":
    pipeline = CDCPipeline()
    pipeline.run()
