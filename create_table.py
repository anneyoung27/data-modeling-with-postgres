import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to sparkify database
    try:
        conn = psycopg2.connect(
            user="postgres",\
            password="Akusukabaju123",\
            host="localhost",\
            port="5432",\
            dbname="sparkifydb"
        )
        print(f"[INFO] Successfully connected to PostgreSQL and Database created!")
    except:
        print(f"[INFO] Connection to the PostgreSQL database failed!")
        
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()