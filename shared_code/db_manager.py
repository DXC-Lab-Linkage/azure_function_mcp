import logging
import os

import psycopg2
from psycopg2 import pool

# Initialize the connection pool once when the module is loaded.
# This happens when the Function App worker starts.
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

            # minconn=1, maxconn=20 (adjust as needed)
            # These numbers depend on your expected concurrency and PostgreSQL server capacity.
            db_pool = pool.SimpleConnectionPool(1, 20, conn_str)
            logging.info("Database connection pool initialized successfully.")
        except (Exception, psycopg2.Error) as error:
            logging.error(
                f"Error while connecting to PostgreSQL or initializing pool: {error}"
            )
            db_pool = None  # Ensure pool is None if initialization fails
    return db_pool


def get_db_connection():
    """Gets a connection from the pool."""
    global db_pool
    if db_pool is None:
        # Attempt to initialize if not already done (e.g., if first call failed)
        init_db_pool()
        if db_pool is None:  # Still None after attempting init
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
            # Optionally, you might want to close the connection if putting it back fails
            # conn.close()


# Call init_db_pool() when the module is first imported by the Function App worker.
# This ensures the pool is ready before any function tries to use it.
init_db_pool()
