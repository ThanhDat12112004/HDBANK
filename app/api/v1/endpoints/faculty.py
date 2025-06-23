from fastapi import APIRouter, Depends, HTTPException
from app.db.session import get_db
from app.schemas.faculty import Faculty
from app.services import faculty_service

router = APIRouter(prefix="/faculties", tags=["faculties"])

@router.post("/")
async def create_faculty_endpoint(faculty: Faculty, db=Depends(get_db)):
    result = await faculty_service.create_faculty(db, faculty)
    if result.get("message") == "Thêm khoa không thành công":
        raise HTTPException(status_code=400, detail=result.get("error", "Lỗi khi thêm khoa"))
    return result

@router.get("/{facultyId}")
async def get_faculty_endpoint(facultyId: str, db=Depends(get_db)):
    try:
        result = await faculty_service.get_faculty(db, facultyId)
        if not result.get("data"):
            raise HTTPException(status_code=404, detail="Không tìm thấy khoa")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.get("/")
async def get_all_faculties_endpoint(db=Depends(get_db)):
    result = await faculty_service.get_all_faculties(db)
    if not result.get("data"):
        raise HTTPException(status_code=404, detail="Không tìm thấy khoa")
    return result

@router.put("/{facultyId}")
async def update_faculty_endpoint(facultyId: str, faculty: Faculty, db=Depends(get_db)):
    try:
        result = await faculty_service.update_faculty(db, facultyId, faculty)
        if result.get("message") == "Cập nhật khoa không thành công":
            raise HTTPException(status_code=404, detail="Không tìm thấy khoa")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.delete("/{facultyId}")
async def delete_faculty_endpoint(facultyId: str, db=Depends(get_db)):
    result = await faculty_service.delete_faculty(db, facultyId)
    if result.get("message") == "Không tìm thấy khoa để xóa":
        raise HTTPException(status_code=404, detail="Không tìm thấy khoa")
    return result