import json
import psycopg2
from db_connection import connect_to_db

def lambda_handler(event, context):
    try:
        # Get student ID from path parameters
        student_id = event['pathParameters']['id']
        
        # Parse the request body
        body = json.loads(event['body'])
        name = body.get('name')
        age = body.get('age')
        
        if not all([name, age]):
            return {
                'statusCode': 400,
                'body': json.dumps('Missing required fields')
            }
        
        connection, cursor = connect_to_db()
        if not connection:
            return {
                'statusCode': 500,
                'body': json.dumps('Database connection failed')
            }
        
        # Update student
        update_query = """
            UPDATE students 
            SET name = %s, age = %s
            WHERE id = %s
        """
        cursor.execute(update_query, (name, age, student_id))
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
            'body': json.dumps('Student updated successfully')
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
