import psycopg2
import config
from faker import Faker

def create_table_and_seed():
    print("Connecting to Postgres...")
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

        # Create table
        print("Creating customers table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                city VARCHAR(100),
                signup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Enable REPLICA IDENTITY FULL to get old values on updates if needed (good practice for CDC)
        cur.execute("ALTER TABLE customers REPLICA IDENTITY FULL;")

        # Seed data
        fake = Faker()
        print("Seeding sample data...")
        for _ in range(10):
            f_name = fake.first_name()
            l_name = fake.last_name()
            email = f"{f_name.lower()}.{l_name.lower()}@example.com"
            city = fake.city()
            
            cur.execute(
                "INSERT INTO customers (first_name, last_name, email, city) VALUES (%s, %s, %s, %s)",
                (f_name, l_name, email, city)
            )
        
        print("Database setup complete.")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error connecting to database: {e}")

if __name__ == "__main__":
    create_table_and_seed()
