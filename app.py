from flask import Flask, render_template
import psycopg2, os 
from reprocess import startReprocess


connection_params = {
    'host': os.getenv("DB_HOST"),        
    'database': os.getenv("DB_NAME"),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': 5432
}

app = Flask(__name__)

def db_connect():
    conn = psycopg2.connect(**connection_params)
    return conn

def fetchQuery(query):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows  

@app.route("/")
def mainPage():
    startReprocess()
    unique_numbers = fetchQuery('SELECT * from unique_numbers')
    top_revunes_data = fetchQuery("""
        SELECT date, amount, data_source
        FROM (SELECT 
                    timestamp as date, 
                    SUM(paid_price) as amount, 
                    data_source,
                    ROW_NUMBER() OVER (PARTITION BY data_source ORDER BY SUM(paid_price) DESC) as rn
                FROM orders
                GROUP BY timestamp, data_source)
        WHERE rn <= 5
        ORDER BY data_source, amount DESC
    """)
    popular_author = fetchQuery("""
        SELECT author, book_sold, data_source  
            FROM                  
                (SELECT b.author, COUNT(o.quantity) as book_sold, o.data_source,
                        ROW_NUMBER() OVER (PARTITION BY o.data_source ORDER BY COUNT(o.quantity) DESC) as rn
                    FROM orders as o 
                        LEFT JOIN books as b 
                            ON o.book_id = b.id
                GROUP BY b.author, o.data_source)
        WHERE rn = 1
        ORDER BY book_sold DESC           
    """)
    best_buyer = fetchQuery("""
        SELECT id, name, duplicated_user_ids, paid_price, data_source  
            FROM                  
                (SELECT u.id, u.name, u.duplicated_user_ids, SUM(o.paid_price) as paid_price, o.data_source,
                        ROW_NUMBER() OVER (PARTITION BY o.data_source ORDER BY SUM(o.paid_price) DESC) as rn
                    FROM orders as o 
                        LEFT JOIN users as u 
                            ON o.user_id = u.id
                GROUP BY u.id, u.name, u.duplicated_user_ids, o.data_source)
        WHERE rn = 1
        ORDER BY data_source, paid_price DESC           
    """)
    context = {
        'unique_numbers': unique_numbers,
        'top_revunes_data': top_revunes_data,
        'popular_author': popular_author,
        'best_buyer': best_buyer
    }
    return render_template("index.html", **context)

if __name__ == "__main__":
    app.run("127.0.0.1", port=5000, debug=True)