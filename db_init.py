import psycopg2, os 
from dotenv import load_dotenv


load_dotenv()

connection_params = {
    'host': 'localhost',        
    'database': os.getenv("DB_NAME"),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': 5432
}

connection = psycopg2.connect(**connection_params)

cur = connection.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS unique_numbers (
        title               TEXT,
        value               TEXT,
        data_source         VARCHAR(5) 
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS books (
        id                  BIGINT,
        title               TEXT,
        author              TEXT,
        genre               TEXT,
        publisher           TEXT,
        year                INTEGER,
        data_source         VARCHAR(5) 
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id                      BIGINT,
        name                    TEXT,
        address                 TEXT,
        phone                   TEXT,
        email                   TEXT,
        duplicated_user_ids     TEXT,
        data_source             VARCHAR(5) 
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        user_id             BIGINT, 
        book_id             BIGINT,
        quantity            INTEGER,
        unit_price          DECIMAL(10, 2),
        paid_price          DECIMAL(10, 2),
        timestamp           DATE,
        shipping            TEXT,
        data_source         VARCHAR(5),
        currency_type       VARCHAR(3)
    )
""")

cur.close()
connection.commit()
connection.close()