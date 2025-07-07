from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import pyodbc

Base = declarative_base()

class SQLAlchemyHelper:
    def __init__(self, database_uri):
        self.engine = create_engine(database_uri)
        self.Session = sessionmaker(bind=self.engine)
        self.create_tables()

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def insert_data(self, data_objects):
        session = self.Session()
        try:
            session.add_all(data_objects)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error occurred: {e}")
        finally:
            session.close()

    def query_data(self, model_class):
        session = self.Session()
        try:
            results = session.query(model_class).all()
            df = pd.DataFrame([item.__dict__ for item in results])
            if '_sa_instance_state' in df.columns:
                df = df.drop('_sa_instance_state', axis=1)
            return df
        except SQLAlchemyError as e:
            print(f"Error occurred: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def close_connection(self):
        self.engine.dispose()

def get_sql_table_data(connection_string, table_name):
    """
    Fetch all data from a specified SQL Server table.

    :param connection_string: The connection string to connect to the SQL Server
    :param table_name: The name of the table to retrieve data from
    :return: A list of tuples, each representing a row of data
    """
    # Establish a connection to the SQL Server
    conn = pyodbc.connect(connection_string)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Define the SQL query to fetch all data from the specified table
    query = f"SELECT * FROM {table_name}"
    
    try:
        # Execute the query
        cursor.execute(query)
        
        # Fetch column names
        column_names = [column[0] for column in cursor.description]
        
        # Fetch all rows from the query result
        rows = cursor.fetchall()
        
        # Return the column names and rows as a tuple
        return column_names, rows

    except pyodbc.Error as e:
        # Handle errors in SQL execution
        print(f"Error fetching data: {e}")
        return None

    finally:
        # Close the cursor and the connection
        cursor.close()
        conn.close()

def readfile(file_path):
    try:
        df = pd.read_csv(file_path, sep='|', header=None)  # Adjust header if your file has headers
        print("Data read into DataFrame successfully.")
        print(df.head())  # Display the first few rows of the DataFrame
    except FileNotFoundError:
        print(f"The file at {file_path} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Usage example
if __name__ == "__main__":
    DATABASE_URI = 'mssql+pyodbc://@LAPTOP-3KHISIU8\SQLEXPRESS/testdb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    db_helper = SQLAlchemyHelper(DATABASE_URI)

    connection_string = "Driver={SQL Server};Server=LAPTOP-3KHISIU8\SQLEXPRESS;Database=testdb;Trusted_Connection=yes;"
    table_name = "tablenew"
    
    # data = [
    #     [1, 'test1', 2.345],
    #     [2, 'test1', 4.665000000000],
    #     [3, 'test3', None]
    # ]
    # example_df = pd.DataFrame(data, columns=['id', 'name', 'value'])
    # print(example_df)
    # tbl_data = get_sql_table_data(connection_string, table_name)
    # data_df = pd.DataFrame(tbl_data)
    # print(data_df)

    data_df = readfile('C:\\Users\\Hp\\Documents\\datfile\\test.dat')
    print(data_df)

    engine = create_engine(connection_string)  # Replace with your actual database

    # For PostgreSQL (example)
    # engine = create_engine('postgresql://username:password@localhost:5432/your_database')

    # Insert DataFrame data into SQL database
    data_df.to_sql('tablenew', engine, if_exists='replace', index=False)  
    




