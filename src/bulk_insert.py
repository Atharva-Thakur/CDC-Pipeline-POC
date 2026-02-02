import psycopg2
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

def bulk_insert(count=5, delay=2.0):
    """
    Inserts 'count' records with a 'delay' (seconds) between them.
    This helps observe the real-time CDC logs clearly.
    """
    logger.info(f"Connecting to Postgres to insert {count} records...")
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
        
        for i in range(1, count + 1):
            start_time = time.time()
            f_name = fake.first_name()
            l_name = fake.last_name()
            # Ensure unique emails mostly
            email = f"{f_name.lower()}.{l_name.lower()}{random.randint(1,9999)}@test.com"
            city = fake.city()
            
            sql = "INSERT INTO customers (first_name, last_name, email, city) VALUES (%s, %s, %s, %s)"
            cur.execute(sql, (f_name, l_name, email, city))
            
            insert_duration = time.time() - start_time
            logger.info(f"[{i}/{count}] Inserted: {f_name} {l_name} (City: {city}) - Time: {insert_duration:.4f}s")
            
            # Sleep to allow observing the pipeline logs
            time.sleep(delay)
            
        logger.info("Bulk insert complete.")
        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    # Insert 5 records with a 2-second pause between each
    bulk_insert(count=50000000, delay=0)
