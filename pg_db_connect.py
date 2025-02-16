import psycopg2
import os
from dotenv import load_dotenv

def get_database_connection():  
    load_dotenv()

    db_name = os.getenv('POSTGRES_DB')
    db_usr = os.getenv('POSTGRES_USER')
    db_psswrd = os.getenv('POSTGRES_PASSWORD')

    # Start the connection
    conn = psycopg2.connect(
        database=db_name,
        user=db_usr,
        password=db_psswrd,
        host="localhost",
        port="6543"
    )
    #cursor = conn.cursor()
    return conn
'''
conn = get_database_connection()
cursor = conn.cursor()

# Run operations...
cursor.execute("DROP TABLE IF EXISTS pmcArticles;")
cursor.execute("CREATE EXTENSION vector;")

cursor.execute("""
CREATE TABLE pmcArticles (
    pmc VARCHAR(16),
    title TEXT,
	abstract TEXT,
	journal_title VARCHAR(255),
	article_type VARCHAR(100),
	article_ids VARCHAR(20)[],
	authors VARCHAR(100)[],
	affiliations VARCHAR(255)[],
	publication_dates date[]);
""")

cursor.execute("""
INSERT INTO pmcArticles;
""")


cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
""")

# Fetch all the table names
table_names = cursor.fetchall()

# Print the table names
for table_name in table_names:
    print(table_name[0])

cursor.execute("SELECT * FROM pmcArticles;")
colnames = [desc[0] for desc in cursor.description]
print(colnames)
#rows = cursor.fetchall()
#print(rows)
cursor.execute("DROP TABLE IF EXISTS pmcArticles;")

# Closing the cursor and connection
cursor.close()
conn.close()
'''