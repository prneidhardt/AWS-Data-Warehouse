import configparser
import psycopg2
from psycopg2 import Error
from sql_queries import create_table_queries, drop_table_queries
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info(f"Executed drop table query: {query}")
        except Error as e:
            logging.error(f"Error occurred while executing query: {query}")
            logging.error(e)
            conn.rollback()

def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            logging.info(f"Executed create table query: {query}")
        except Error as e:
            logging.error(f"Error occurred while executing query: {query}")
            logging.error(e)
            conn.rollback()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        with psycopg2.connect("host={} dbname={} user={} password={} port={}"\
                            .format(*config['CLUSTER'].values())) as conn:
            cur = conn.cursor()

            drop_tables(cur, conn)
            create_tables(cur, conn)

    except Error as e:
        logging.error(f"Error occurred while connecting to the database")
        logging.error(e)

if __name__ == "__main__":
    main()
