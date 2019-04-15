#importing libaries
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

#The function connects to PostgreSQL; checks if the database already exists, and if so, drops it - to be disabled in production.
def create_database():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn

#The function envokes the drop tables query execution from the sql_queries.py file - to be disabled in production.
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

#The function envokes the create tables query execution from the sql_queries.py file - to be disabled in production.
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

#The function closes the database connection and cursor object.
def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()