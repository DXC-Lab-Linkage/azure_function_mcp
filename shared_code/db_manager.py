import logging
import os

import psycopg2
from psycopg2 import pool

db_pool = None


def init_db_pool():
    global db_pool
    if db_pool is None:
        try:
            conn_str = os.environ.get("POSTGRES_CONNECTION_STRING")
            if not conn_str:
                logging.error(
                    "DB_CONNECTION_STRING environment variable not set."
                )
                raise ValueError(
                    "Database connection string is not configured."
                )

            db_pool = pool.SimpleConnectionPool(1, 20, conn_str)
            logging.info("Database connection pool initialized successfully.")
        except (Exception, psycopg2.Error) as error:
            logging.error(
                f"Error while connecting to PostgreSQL or initializing pool: {error}"
            )
            db_pool = None
    return db_pool


def get_db_connection():
    """Gets a connection from the pool."""
    global db_pool
    if db_pool is None:
        init_db_pool()
        if db_pool is None:
            raise ConnectionError(
                "Database pool is not initialized. Check logs for errors."
            )
    try:
        conn = db_pool.getconn()
        if conn:
            logging.debug("Retrieved a connection from the pool.")
            return conn
        else:
            logging.error(
                "Failed to get connection from pool, pool might be exhausted or broken."
            )
            raise ConnectionError("Failed to get connection from pool.")
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error getting connection from pool: {error}")
        raise


def release_db_connection(conn):
    """Releases a connection back to the pool."""
    global db_pool
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
            logging.debug("Released a connection back to the pool.")
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error releasing connection to pool: {error}")


init_db_pool()
