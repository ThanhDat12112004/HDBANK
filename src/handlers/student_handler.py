import json
from typing import Dict, Any
import psycopg2.extras
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models.db import get_db_connection
from models.student import Student

logger = Logger()

def create_response(status_code: int, body: Any) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "body": json.dumps(body),
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        }
    }

@logger.inject_lambda_context
def get_students(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM students")
        students = cur.fetchall()
        cur.close()
        conn.close()
        
        return create_response(200, students)
    except Exception as e:
        logger.error(f"Error getting students: {str(e)}")
        return create_response(500, {"error": "Internal Server Error"})

@logger.inject_lambda_context
def get_student(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    try:
        student_id = event['pathParameters']['id']
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM students WHERE id = %s", (student_id,))
        student = cur.fetchone()
        cur.close()
        conn.close()
        
        if not student:
            return create_response(404, {"error": "Student not found"})
        
        return create_response(200, student)
    except Exception as e:
        logger.error(f"Error getting student: {str(e)}")
        return create_response(500, {"error": "Internal Server Error"})

@logger.inject_lambda_context
def create_student(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    try:
        body = json.loads(event['body'])
        student = Student(**body)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            INSERT INTO students (name, age, grade, email)
            VALUES (%s, %s, %s, %s)
            RETURNING *
            """,
            (student.name, student.age, student.grade, student.email)
        )
        new_student = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        return create_response(201, new_student)
    except Exception as e:
        logger.error(f"Error creating student: {str(e)}")
        return create_response(500, {"error": "Internal Server Error"})

@logger.inject_lambda_context
def update_student(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    try:
        student_id = event['pathParameters']['id']
        body = json.loads(event['body'])
        student = Student(**body)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            UPDATE students
            SET name = %s, age = %s, grade = %s, email = %s
            WHERE id = %s
            RETURNING *
            """,
            (student.name, student.age, student.grade, student.email, student_id)
        )
        updated_student = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        if not updated_student:
            return create_response(404, {"error": "Student not found"})
        
        return create_response(200, updated_student)
    except Exception as e:
        logger.error(f"Error updating student: {str(e)}")
        return create_response(500, {"error": "Internal Server Error"})

@logger.inject_lambda_context
def delete_student(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    try:
        student_id = event['pathParameters']['id']
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("DELETE FROM students WHERE id = %s RETURNING *", (student_id,))
        deleted_student = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        if not deleted_student:
            return create_response(404, {"error": "Student not found"})
        
        return create_response(200, {"message": "Student deleted successfully"})
    except Exception as e:
        logger.error(f"Error deleting student: {str(e)}")
        return create_response(500, {"error": "Internal Server Error"})
