from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Table1(Base):
    __tablename__ = 'table1'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(Integer)
    address = Column(String)

class Table2(Base):
    __tablename__ = 'table2'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(Integer)
    address = Column(String)

# Create an engine and a session for SQL Server
engine = create_engine('mssql+pyodbc://@LAPTOP-3KHISIU8\SQLEXPRESS/testdb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')
Session = sessionmaker(bind=engine)
session = Session()

# Create tables (if they don't exist)
Base.metadata.create_all(engine)

def compare_table_columns(table_class1, table_class2, column_name):
    """
    Compare data of the same column in two different tables.

    :param table_class1: The ORM class representing the first table.
    :param table_class2: The ORM class representing the second table.
    :param column_name: The name of the column to compare.
    :return: A dictionary with differences.
    """
    rows1 = session.query(table_class1).all()
    rows2 = session.query(table_class2).all()

    differences = {}
    for row1, row2 in zip(rows1, rows2):
        value1 = getattr(row1, column_name)
        value2 = getattr(row2, column_name)
        if value1 != value2:
            differences[(row1.id, row2.id)] = (value1, value2)

    return differences

def compare_all_columns(table_class1, table_class2):
    """
    Compare data of all columns in two different tables.

    :param table_class1: The ORM class representing the first table.
    :param table_class2: The ORM class representing the second table.
    :return: A dictionary with differences.
    """
    rows1 = session.query(table_class1).all()
    rows2 = session.query(table_class2).all()

    differences = {}
    max_len = max(len(rows1), len(rows2))
    for i in range(max_len):
        row_diff = {}
        row1 = rows1[i] if i < len(rows1) else None
        row2 = rows2[i] if i < len(rows2) else None

        if row1 and row2:
            for column in table_class1.__table__.columns:
                col_name = column.name
                value1 = getattr(row1, col_name)
                value2 = getattr(row2, col_name)
                if value1 != value2:
                    row_diff[col_name] = (value1, value2)
        elif row1:
            row_diff = {column.name: (getattr(row1, column.name), None) for column in table_class1.__table__.columns}
        elif row2:
            row_diff = {column.name: (None, getattr(row2, column.name)) for column in table_class2.__table__.columns}

        if row_diff:
            differences[(row1.id if row1 else None, row2.id if row2 else None)] = row_diff

    return differences

# Example usage:
# Insert sample data
# session.add_all([
#     Table1(id=1, name='Alice', value=10,address='abcd'),
#     Table1(id=2, name='Bob', value=20,address='xyz'),
#     Table2(id=1, name='Alices', value=30,address='abcd'),
#     Table2(id=2, name='Bob', value=20,address='xyz'),
#     Table2(id=3, name='Jac', value=30,address='grw')
# ])
# session.commit()

# Compare the 'value' column in both tables
# differences = compare_table_columns(Table1, Table2, 'value')
differences = compare_all_columns(Table1, Table2)
print(differences)