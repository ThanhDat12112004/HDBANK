
import json
import psycopg2
from db_connection import connect_to_db

def lambda_handler(event, context):
    try:
        # Parse the request body
        body = json.loads(event['body'])
        student_id = body.get('mssv')
        name = body.get('name')

        # Kiểm tra các trường bắt buộc
        if not all([student_id, name]):
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps('Missing required fields: mssv, name, or class')
            }

        # Kết nối database
        connection = connect_to_db()
        if not connection:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps('Database connection failed')
            }

        # Tạo cursor và thực thi truy vấn
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO students (mssv, name)
            VALUES (%s, %s)
        """
        cursor.execute(insert_query, (student_id, name))
        connection.commit()

        # Đóng cursor và connection
        cursor.close()

        return {
            'statusCode': 201,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps('Student created successfully')
        }

    except Exception as e:
        # Đóng connection nếu có lỗi
        if 'connection' in locals():
            if 'cursor' in locals():
                cursor.close()
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(f'Error: {str(e)}')
        }
