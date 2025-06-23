from fastapi import APIRouter, Depends, HTTPException
from app.db.session import get_db
from app.schemas.grade import Grade
from app.services import grade_service

router = APIRouter(prefix="/grades", tags=["grades"])

@router.post("/")
async def create_grade_endpoint(grade: Grade, db=Depends(get_db)):
    result = await grade_service.create_grade(db, grade)
    if result.get("message") == "Thêm điểm số không thành công":
        raise HTTPException(status_code=400, detail=result.get("error", "Lỗi khi thêm điểm số"))
    return result

@router.get("/{gradeId}")
async def get_grade_endpoint(gradeId: int, db=Depends(get_db)):
    try:
        result = await grade_service.get_grade(db, gradeId)
        if not result.get("data"):
            raise HTTPException(status_code=404, detail="Không tìm thấy điểm số")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.get("/")
async def get_all_grades_endpoint(db=Depends(get_db)):
    result = await grade_service.get_all_grades(db)
    if not result.get("data"):
        raise HTTPException(status_code=404, detail="Không tìm thấy điểm số")
    return result

@router.put("/{gradeId}")
async def update_grade_endpoint(gradeId: int, grade: Grade, db=Depends(get_db)):
    try:
        result = await grade_service.update_grade(db, gradeId, grade)
        if result.get("message") == "Cập nhật điểm số không thành công":
            raise HTTPException(status_code=404, detail="Không tìm thấy điểm số")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.delete("/{gradeId}")
async def delete_grade_endpoint(gradeId: int, db=Depends(get_db)):
    result = await grade_service.delete_grade(db, gradeId)
    if result.get("message") == "Không tìm thấy điểm số để xóa":
        raise HTTPException(status_code=404, detail="Không tìm thấy điểm số")
    return result