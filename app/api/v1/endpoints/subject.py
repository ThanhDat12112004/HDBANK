from fastapi import APIRouter, Depends, HTTPException
from app.db.session import get_db
from app.schemas.subject import Subject
from app.services import subject_service

router = APIRouter(prefix="/subjects", tags=["subjects"])

@router.post("/")
async def create_subject_endpoint(subject: Subject, db=Depends(get_db)):
    result = await subject_service.create_subject(db, subject)
    if result.get("message") == "Thêm môn học không thành công":
        raise HTTPException(status_code=400, detail=result.get("error", "Lỗi khi thêm môn học"))
    return result

@router.get("/{subjectId}")
async def get_subject_endpoint(subjectId: str, db=Depends(get_db)):
    try:
        result = await subject_service.get_subject(db, subjectId)
        if not result.get("data"):
            raise HTTPException(status_code=404, detail="Không tìm thấy môn học")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.get("/")
async def get_all_subjects_endpoint(db=Depends(get_db)):
    result = await subject_service.get_all_subjects(db)
    if not result.get("data"):
        raise HTTPException(status_code=404, detail="Không tìm thấy môn học")
    return result

@router.put("/{subjectId}")
async def update_subject_endpoint(subjectId: str, subject: Subject, db=Depends(get_db)):
    try:
        result = await subject_service.update_subject(db, subjectId, subject)
        if result.get("message") == "Cập nhật môn học không thành công":
            raise HTTPException(status_code=404, detail="Không tìm thấy môn học")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.delete("/{subjectId}")
async def delete_subject_endpoint(subjectId: str, db=Depends(get_db)):
    result = await subject_service.delete_subject(db, subjectId)
    if result.get("message") == "Không tìm thấy môn học để xóa":
        raise HTTPException(status_code=404, detail="Không tìm thấy môn học")
    return result