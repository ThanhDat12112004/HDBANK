from fastapi import APIRouter, Depends, HTTPException
from app.db.session import get_db
from app.schemas.teacher import Teacher
from app.services import teacher_service

router = APIRouter(prefix="/teachers", tags=["teachers"])

@router.post("/")
async def create_teacher_endpoint(teacher: Teacher, db=Depends(get_db)):
    result = await teacher_service.create_teacher(db, teacher)
    if result.get("message") == "Thêm giáo viên không thành công":
        raise HTTPException(status_code=400, detail=result.get("error", "Lỗi khi thêm giáo viên"))
    return result

@router.get("/{teacherId}")
async def get_teacher_endpoint(teacherId: str, db=Depends(get_db)):
    try:
        result = await teacher_service.get_teacher(db, teacherId)
        if not result.get("data"):
            raise HTTPException(status_code=404, detail="Không tìm thấy giáo viên")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.get("/")
async def get_all_teachers_endpoint(db=Depends(get_db)):
    result = await teacher_service.get_all_teachers(db)
    if not result.get("data"):
        raise HTTPException(status_code=404, detail="Không tìm thấy giáo viên")
    return result

@router.put("/{teacherId}")
async def update_teacher_endpoint(teacherId: str, teacher: Teacher, db=Depends(get_db)):
    try:
        result = await teacher_service.update_teacher(db, teacherId, teacher)
        if result.get("message") == "Cập nhật giáo viên không thành công":
            raise HTTPException(status_code=404, detail="Không tìm thấy giáo viên")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.delete("/{teacherId}")
async def delete_teacher_endpoint(teacherId: str, db=Depends(get_db)):
    result = await teacher_service.delete_teacher(db, teacherId)
    if result.get("message") == "Không tìm thấy giáo viên để xóa":
        raise HTTPException(status_code=404, detail="Không tìm thấy giáo viên")
    return result