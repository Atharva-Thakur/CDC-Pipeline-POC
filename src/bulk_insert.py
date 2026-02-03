import psycopg2
import psycopg2.extras
import config
import time
import random
import logging
from faker import Faker

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
logger = logging.getLogger("Bulk_Insert")

def bulk_insert(batch_size=100, batches=1, delay=2.0):
    """
    Inserts records in batches.
    Total records = batch_size * batches.
    """
    logger.info(f"Connecting to Postgres to insert {batch_size * batches} customer+address pairs in {batches} batches...")
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        fake = Faker()
        
        for b in range(1, batches + 1):
            start_time = time.time()
            
            # We must insert customers first to get IDs, then addresses.
            # execute_values is great but doesn't easily return IDs for bulk inserts in a way matching input order guaranteed across all PG versions safely without care.
            # For simplicity in this script, we will do a loop or use 'RETURNING id' carefully.
            # Let's do a loop for safety and clarity in this POC.
            
            for _ in range(batch_size):
                f_name = fake.first_name()
                l_name = fake.last_name()
                email = f"{f_name.lower()}.{l_name.lower()}{random.randint(1,99999)}@test.com"
                
                cur.execute(
                    "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
                    (f_name, l_name, email)
                )
                c_id = cur.fetchone()[0]

                street = fake.street_address()
                city = fake.city()
                zip_code = fake.zipcode()

                cur.execute(
                     "INSERT INTO addresses (customer_id, street, city, zip_code) VALUES (%s, %s, %s, %s)",
                     (c_id, street, city, zip_code)
                )
            
            insert_duration = time.time() - start_time
            logger.info(f"[Batch {b}/{batches}] Inserted {batch_size} pairs - Time: {insert_duration:.4f}s")
            
            if b < batches:
                time.sleep(delay)
            
        logger.info("Bulk insert complete.")
        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    # Insert 1000 records total (10 batches of 100)
    bulk_insert(batch_size=100, batches=10, delay=0.0)
