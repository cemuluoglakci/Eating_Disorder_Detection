import sqlalchemy
import pymysql
import time

class Sql():
    def __init__(self, settings) -> None:
        self.settings=settings
        connectionString = self.createConnectionString()
        self.engine = sqlalchemy.create_engine(connectionString)
        self.metadata = sqlalchemy.MetaData()
        self.connect()


    def connect(self):
        self.connection = self.engine.connect()
        return self.connection

    def setTableDefinition(self, tableName):
        return sqlalchemy.Table(
            tableName, 
            self.metadata, 
            autoload=True, 
            autoload_with=self.engine)

    def execute(self, statement):
        #Retry mechanism implemented as pymysql library usually gives connection error.
        for i in range(5):
            try:
                self.connection.execute(statement)
                break
            except pymysql.err.OperationalError as error:
                print(f"Database connection lost. Retrying... \n{str(error)}")
                time.sleep(0.3)
                continue

            

    def createConnectionString(self):
        return ("mysql+pymysql://"
        f"{self.settings.DB_USER}:"
        f"{self.settings.DB_PASSWORD}@"
        f"{self.settings.DB_HOST}:"
        f"{self.settings.DB_PORT}/"
        f"{self.settings.DB}")