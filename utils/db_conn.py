import pyodbc

from typing import List, Tuple

from config import logging
from decorators.db_decorator import recursion_limiter

'''
Mssql requires driver with different platforms

[unixODBC][Driver Manager]Can't open lib 'ODBC Driver 17 for SQL Server' : file not found (0) (SQLDriverConnect)")

'''

class DatabaseConnector:
    
    def __init__(self, 
                 server: str, 
                 database: str, 
                 username: str, 
                 password: str,
                 port: int = 8081):
        
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port

        self.conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "TrustServerCertificate=Yes;"
            )
        
        self.conn = None
        self.cursor = None
        
        self.logger = logging.getLogger(__name__)

        self._execution_history: List[Tuple[str, bool]] = []

        self.SQL_fetch_tables = f"""SELECT * FROM sys.tables"""

    @recursion_limiter(max_depth=3)
    def connect(self):
        self.logger.debug("Attempting to connect to the database.")
        
        if self.conn is not None:
            self.logger.info("Connection already exists.")
            return
        
        print(self.conn_str)

        try:
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            self.logger.info("Connected to the database successfully.")
        except pyodbc.Error as e:
            self.logger.error(f"Error connecting to the database: {e}")
            self.connect()

    def execute(self, query: str):
        self.logger.debug(f"Executing query: {query}")
        
        if self.conn is None:
            self.logger.error("No connection found. Please connect first.")
            return
        
        rows = None
        try:
            self.cursor.execute(query)
            if (not query.startswith("SELECT")):
                self.conn.commit()
            else:
                rows = self.cursor.fetchall()
            
            self._execution_history.append((query, True))
            self.logger.info(f"Query executed successfully")
            return rows
        except pyodbc.Error as e:
            self._execution_history.append((query, True))
            self.logger.warning(f"Error executing query: {e}")
            self.conn.rollback()

    def close(self):
        self.logger.debug("Attempting to close the database connection.")
        
        if self.conn is None:
            self.logger.error("No connection found. Please connect first.")
            return
        
        try:
            self.cursor.close()
            self.conn.close()
            self.conn = None
            self.cursor = None
            self.logger.info("Database connection closed successfully.")
        except pyodbc.Error as e:
            self.logger.error(f"Error closing the database connection: {e}")
