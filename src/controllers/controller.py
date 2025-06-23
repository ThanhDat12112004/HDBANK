from src.models.studentModel import Student
from src.services.service import (
    create_student_db,
    get_student_db,
    get_all_students_db,
    update_student_db,
    delete_student_db,
)

async def create_student(db, student: Student):
    if not student.hovaten or not student.tuoi:
        raise ValueError("Name and age are required")
    return await create_student_db(db, student)

async def get_student(db, mssv: int):
    student = await get_student_db(db, mssv)
    if not student:
        raise ValueError("Student not found")
    return student

async def get_all_students(db):
    return await get_all_students_db(db)

async def update_student(db, mssv: int, student: Student):
    if not student.hovaten or not student.tuoi:
        raise ValueError("Name and age are required")
    updated_student = await update_student_db(db, mssv, student)
    if not updated_student:
        raise ValueError("Student not found")
    return updated_student

async def delete_student(db, mssv: int):
    return await delete_student_db(db, mssv)
