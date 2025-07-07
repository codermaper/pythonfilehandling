from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

# Base class for ORM
Base = declarative_base()

# DBConnect Class
class DBConnect:
    def __init__(self, database_uri):
        self.database_uri = 'mssql+pyodbc://@LAPTOP-3KHISIU8\SQLEXPRESS/testdb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
        self.engine = None
        self.Session = None
        
    def connect(self):
        self.engine = create_engine(self.database_uri)
        self.Session = sessionmaker(bind=self.engine)
        
    def close_connection(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.Session = None

# DatabaseHelper Class
class DatabaseHelper:
    def __init__(self, db_connect):
        self.db_connect = db_connect
        
    def create_tables(self):
        Base.metadata.create_all(self.db_connect.engine)

    def insert_data(self, data_objects):
        session = self.db_connect.Session()
        try:
            session.add_all(data_objects)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error occurred: {e}")
        finally:
            session.close()

    def query_data(self, model_class):
        session = self.db_connect.Session()
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

# Employee Model Class
class Employee(Base):
    __tablename__ = 'employees'
    employee_id = Column(Integer, Sequence('employee_id_seq'), primary_key=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    age = Column(Integer, nullable=False)
    department = Column(String(50), nullable=False)

# Usage Example
if __name__ == "__main__":
    
    
    # Create a DBConnect instance and connect to the database
    db_connect = DBConnect()
    db_connect.connect()
    
    # Create a DatabaseHelper instance
    db_helper = DatabaseHelper(db_connect)
    
    # Create tables
    db_helper.create_tables()

    # Sample data
    employees = [
        Employee(employee_id=1, first_name='John', last_name='Doe', age=28, department='Engineering'),
        Employee(employee_id=2, first_name='Jane', last_name='Smith', age=32, department='HR'),
        Employee(employee_id=3, first_name='Emily', last_name='Davis', age=40, department='Finance'),
        Employee(employee_id=4, first_name='Michael', last_name='Johnson', age=25, department='Marketing')
    ]
    db_helper.insert_data(employees)

    # Query data
    df = db_helper.query_data(Employee)
    print(df)

    # Close the database connection
    db_connect.close_connection()
