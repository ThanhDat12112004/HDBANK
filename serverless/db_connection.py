import psycopg2
from psycopg2 import Error

def connect_to_db():
    try:      
        connection = psycopg2.connect(
            host="database-1.cp6ai4kmm7tf.us-east-1.rds.amazonaws.com",
            database="manageStudents",
            user="postgres",
            password="0949927642aB!",
            port=5432
        )
        return connection
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None

