from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

class DatabaseHelper:
    def __init__(self, db_connect):
        self.db_connect = db_connect
        
    def create_tables(self, base):
        base.metadata.create_all(self.db_connect.engine)

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
