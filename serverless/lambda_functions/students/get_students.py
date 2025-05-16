import json
import psycopg2
from db_connection import connect_to_db

def lambda_handler(event, context):
    try:
        connection = connect_to_db()
        if not connection:
            return {
                'statusCode': 500,
                'body': json.dumps('Database connection failed')
            }
        connection = connection.cursor()
        # Get all students
        connection.execute("SELECT * FROM students")
        rows = connection.fetchall()
        
        # Convert to list of dictionaries
        students = []
        for row in rows:
            students.append({
                'id': row[0],
                'name': row[1],
            })
            
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(students)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
