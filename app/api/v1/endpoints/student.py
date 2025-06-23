from fastapi import APIRouter, Depends, HTTPException
from app.db.session import get_db
from app.schemas.student import Student
from app.services import student_service
from app.api.dependencies.auth import get_current_user

router = APIRouter(prefix="/students", tags=["students"])

@router.post("/", response_model=Student)
async def create_student_endpoint(
    student: Student, 
    db=Depends(get_db),
    current_user: str = Depends(get_current_user)
):
    try:
        result = await student_service.create_student(db, student)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{studentId}", response_model=Student)
async def get_student_endpoint(
    studentId: str, 
    db=Depends(get_db),
    current_user: str = Depends(get_current_user)
):
    try:
        result = await student_service.get_student(db, studentId)
        if result:
            return result
        raise HTTPException(status_code=404, detail="Không tìm thấy sinh viên")
    except ValueError as ve:
        raise HTTPException(status_code=404, detail="Không tìm thấy sinh viên")

@router.get("/", response_model=list[Student])
async def get_all_students_endpoint(
    db=Depends(get_db),
    current_user: str = Depends(get_current_user)
):
    result = await student_service.get_all_students(db)
    if not result:
        raise HTTPException(status_code=404, detail="Không tìm thấy sinh viên")
    return [Student(**student) for student in result]

@router.put("/{studentId}")
async def update_student_endpoint(
    studentId: str, 
    student: Student, 
    db=Depends(get_db),
    current_user: str = Depends(get_current_user)
):
    try:
        result = await student_service.update_student(db, studentId, student)
        if result.get("message") == "Cập nhật sinh viên không thành công":
            raise HTTPException(status_code=404, detail="Không tìm thấy sinh viên")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.delete("/{studentId}")
async def delete_student_endpoint(
    studentId: str, 
    db=Depends(get_db),
    current_user: str = Depends(get_current_user)
):
    result = await student_service.delete_student(db, studentId)
    if result.get("message") == "Không tìm thấy sinh viên để xóa":
        raise HTTPException(status_code=404, detail="Không tìm thấy sinh viên")
    return result