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

        # Clean slate
        print("Dropping existing tables...")
        cur.execute("DROP TABLE IF EXISTS addresses;")
        cur.execute("DROP TABLE IF EXISTS customers;")

        # Create customers table
        print("Creating customers table...")
        cur.execute("""
            CREATE TABLE customers (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                signup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create addresses table
        print("Creating addresses table...")
        cur.execute("""
            CREATE TABLE addresses (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
                street VARCHAR(200),
                city VARCHAR(100),
                zip_code VARCHAR(20)
            );
        """)

        # Enable REPLICA IDENTITY FULL for both to ensure we get keys on deletes/updates
        cur.execute("ALTER TABLE customers REPLICA IDENTITY FULL;")
        cur.execute("ALTER TABLE addresses REPLICA IDENTITY FULL;")

        # Seed data
        fake = Faker()
        print("Seeding sample data...")
        for _ in range(10):
            f_name = fake.first_name()
            l_name = fake.last_name()
            email = f"{f_name.lower()}.{l_name.lower()}@example.com"
            
            # Insert customer
            cur.execute(
                "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
                (f_name, l_name, email)
            )
            customer_id = cur.fetchone()[0]

            # Insert address linked to customer
            street = fake.street_address()
            city = fake.city()
            zip_code = fake.zipcode()
            cur.execute(
                "INSERT INTO addresses (customer_id, street, city, zip_code) VALUES (%s, %s, %s, %s)",
                (customer_id, street, city, zip_code)
            )
        
        print("Database setup complete.")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error connecting to database: {e}")

if __name__ == "__main__":
    create_table_and_seed()
