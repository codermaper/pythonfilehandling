import pyodbc
import logging
import os

class sql_server_connection(object):
    """SQL Server DB connection"""
    def __init__(self, connection_string=os.environ["SQL_CONN"]):
        self.connection_string = connection_string
        self.connector = None
    
    def __enter__(self):
        self.connector = pyodbc.connect(self.connection_string)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()

def db_connector_sql(func):
    def with_connection_(*args, **kwargs):
        conn_str = os.environ["SQL_CONN"]
        cnn = pyodbc.connect(conn_str)
        try:
            rv = func(cnn, *args, **kwargs)
        except Exception as e:
            cnn.rollback()
            logging.error("Database connection error: %s", e)
            raise
        else:
            cnn.commit()
        finally:
            cnn.close()
        return rv
    return with_connection_
