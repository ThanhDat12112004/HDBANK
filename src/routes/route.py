from fastapi import APIRouter, Depends, HTTPException
from src.config.database import get_db
from src.models.studentModel import Student
from src.controllers.controller import (
    create_student,
    get_student,
    get_all_students,
    update_student,
    delete_student,
)

router = APIRouter()

@router.post("/students/")
async def create_student_endpoint(student: Student, db=Depends(get_db)):
    result = await create_student(db, student)
    return result

@router.get("/students/{mssv}")
async def get_student_endpoint(mssv: int, db=Depends(get_db)):
    result = await get_student(db, mssv)
    return result

@router.get("/students/")
async def get_all_students_endpoint(db=Depends(get_db)):
    return await get_all_students(db)

@router.put("/students/{mssv}")
async def update_student_endpoint(mssv: int, student: Student, db=Depends(get_db)):
    result = await update_student(db, mssv, student)
    if not result:
        raise HTTPException(status_code=404, detail="Student not found")
    return result

@router.delete("/students/{mssv}")
async def delete_student_endpoint(mssv: int, db=Depends(get_db)):
    return await delete_student(db, mssv)

