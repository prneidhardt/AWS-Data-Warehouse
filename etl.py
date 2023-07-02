import configparser
import psycopg2
from psycopg2 import Error
from sql_queries import copy_table_queries, insert_table_queries
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def fetch_table_names(cur):
    try:
        cur.execute("SELECT table_name \
                    FROM information_schema.tables \
                    WHERE table_schema='public'")
        table_names = [row[0] for row in cur.fetchall()]
        return table_names
    except Error as e:
        logging.error("Error occurred while fetching table names")
        logging.error(e)

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info(f"Executed load staging table query: {query}")
        except Error as e:
            logging.error(f"Error occurred while executing query: {query}")
            logging.error(e)
            conn.rollback()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info(f"Executed insert table query: {query}")
        except Error as e:
            logging.error(f"Error occurred while executing query: {query}")
            logging.error(e)
            conn.rollback()

def count_rows(cur, conn):
    table_names = fetch_table_names(cur)
    for table in table_names:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            result = cur.fetchone()
            count = result[0]
            logging.info(f"Row count for {table}: {count}")
        except Error as e:
            logging.error(f"Error occurred while counting rows for {table}")
            logging.error(e)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        with psycopg2.connect("host={} dbname={} user={} password={} port={}"
                              .format(*config['CLUSTER'].values())) as conn:
            cur = conn.cursor()

            load_staging_tables(cur, conn)
            insert_tables(cur, conn)
            count_rows(cur, conn)

    except Error as e:
        logging.error(f"Error occurred while connecting to the database")
        logging.error(e)

if __name__ == "__main__":
    main()
