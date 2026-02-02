import time
import json
import datetime
import re
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
            self.cur.create_replication_slot(slot_name, output_plugin='test_decoding')
            logger.info(f"Replication slot '{slot_name}' created successfully.")
        except psycopg2.errors.DuplicateObject:
            logger.info(f"Replication slot '{slot_name}' already exists. Reusing it.")
        except Exception as e:
            logger.error(f"Error creating slot: {e}")

    def transform_data(self, data_dict):
        """
        Apply transformations:
        1. Combine first_name and last_name to full_name.
        2. Clean strings.
        3. Add metadata.
        """
        transformed = {}
        
        # ID is required (convert to string for search)
        if 'id' in data_dict:
            transformed['id'] = str(data_dict['id'])
        
        # Combine names
        first = data_dict.get('first_name', '').strip("'")
        last = data_dict.get('last_name', '').strip("'")
        transformed['full_name'] = f"{first} {last}".strip()
        
        # Pass through other fields
        if 'email' in data_dict:
            transformed['email'] = data_dict['email'].strip("'")
        if 'city' in data_dict:
            transformed['city'] = data_dict['city'].strip("'")
            
        transformed['processed_at'] = datetime.datetime.utcnow().isoformat() + "Z"
        
        return transformed

    def parse_test_decoding_message(self, payload):
        """
        Parses 'test_decoding' plugin output.
        Expects: "table public.customers: INSERT: id[integer]:1 ..."
        """
        try:
            # We only care about customer table
            if "table public.customers:" not in payload:
                return None, None

            # Detect operation
            op = None
            if "INSERT:" in payload:
                op = "INSERT"
            elif "UPDATE:" in payload:
                op = "UPDATE"
            elif "DELETE:" in payload:
                op = "DELETE"
            
            if not op:
                return None, None

            data = {}
            # Regex to capture key[type]:value
            # This is a basic parser for the POC.
            regex = r"(\w+)\[[\w\s]+\]:('?.*?'?)(?=\s\w+\[|$)"
            matches = re.findall(regex, payload)
            for key, val in matches:
                data[key] = val

            return op, data

        except Exception as e:
            logger.error(f"Error parsing payload: {e}")
            return None, None

    def push_to_search(self, action, data):
        if not self.search_client:
            logger.warning(f"Skipping Search Push. Client not configured.")
            return

        start_time = time.time()
        try:
            logger.info("----- [STEP 3: TRANSFORMATION PHASE] -----")
            logger.info(f"Raw Input Data: {data}")
            
            doc = self.transform_data(data)
            
            logger.info(f"Transformed Payload: {doc}")
            logger.info("Transformation Details: Merged 'first_name' + 'last_name' -> 'full_name'. Added 'processed_at'.")

            logger.info("----- [STEP 4: AZURE INDEXING PHASE] -----")
            if action == "DELETE":
                if 'id' in doc:
                   logger.info(f"Operation: DELETE document with ID: {doc['id']}")
                   # Note: In a real scenario, we might retry on failure
                   self.search_client.delete_documents(documents=[{"id": doc['id']}]) 
                   logger.info("Azure Search Output: Document Deletion Successful.")
            else:
                logger.info(f"Operation: MERGE/UPLOAD")
                logger.info(f"Payload sending to Azure: {json.dumps(doc, default=str)}")
                
                result = self.search_client.merge_or_upload_documents(documents=[doc])
                
                if result and result[0].succeeded:
                    logger.info(f"Azure Search Output: SUCCESS. Key='{result[0].key}', Status Code={result[0].status_code}")
                else:
                    logger.error(f"Azure Search Output: FAILED. {result[0].error_message}")
                
        except Exception as e:
            logger.error(f"Error pushing to search: {e}")
        finally:
            elapsed_time = time.time() - start_time
            logger.info(f"Time taken to process and push to search: {elapsed_time:.4f} seconds")

    def run(self):
        logger.info("Starting CDC Pipeline Service...")
        self.create_replication_slot()
        
        logger.info("Listening for WAL Stream changes on 'cdc_slot'...")
        self.cur.start_replication(slot_name='cdc_slot', decode=True)

        def consume_stream(msg):
            payload = msg.payload

            # Only log interesting events (skip generic BEGIN/COMMIT for cleaner logs)
            if "table public.customers:" in payload:
                logger.info("\n================ NEW CHANGE DETECTED ================")
                logger.info("----- [STEP 1: REPLICATION STREAM] -----")
                logger.info(f"Received (WAL Output): {payload}")

                op, data = self.parse_test_decoding_message(payload)
                if op and data:
                    logger.info("----- [STEP 2: PARSING] -----")
                    logger.info(f"Detected Operation: {op}")
                    logger.info(f"Extracted Data: {data}")
                    
                    self.push_to_search(op, data)
            
            msg.cursor.send_feedback(flush_lsn=msg.data_start)

        self.cur.consume_stream(consume_stream)

if __name__ == "__main__":
    pipeline = CDCPipeline()
    pipeline.run()
