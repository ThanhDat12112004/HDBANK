import json
import psycopg2
from db_connection import connect_to_db

def lambda_handler(event, context):
    try:
        # Get student ID from path parameters
        student_id = event['pathParameters']['id']
        
        connection, cursor = connect_to_db()
        if not connection:
            return {
                'statusCode': 500,
                'body': json.dumps('Database connection failed')
            }
        
        # Delete student
        delete_query = "DELETE FROM students WHERE id = %s"
        cursor.execute(delete_query, (student_id,))
        connection.commit()
        
        if cursor.rowcount == 0:
            return {
                'statusCode': 404,
                'body': json.dumps('Student not found')
            }
        
        close_connection(connection, cursor)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps('Student deleted successfully')
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
