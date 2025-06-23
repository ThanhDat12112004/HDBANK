from fastapi import APIRouter, Depends, HTTPException
from app.db.session import get_db
from app.schemas.class_ import Class
from app.services import class_service

router = APIRouter(prefix="/classes", tags=["classes"])

@router.post("/",)
async def create_class_endpoint(class_: Class, db=Depends(get_db)):
    result = await class_service.create_class(db, class_)
    if result.get("message") == "Thêm lớp học không thành công":
        raise HTTPException(status_code=400, detail=result.get("error", "Lỗi khi thêm lớp học"))
    return result

@router.get("/{classId}",response_model=Class)
async def get_class_endpoint(classId: str, db=Depends(get_db)):
    try:
        result = await class_service.get_class(db, classId)
        return Class(**result)
    except ValueError as ve:
        raise HTTPException(status_code=404)

@router.get("/",response_model=list[Class])
async def get_all_classes_endpoint(db=Depends(get_db)):
    result = await class_service.get_all_classes(db)
    if not result:
        raise HTTPException(status_code=404)
    return [Class(**item) for item in result]

@router.put("/{classId}")
async def update_class_endpoint(classId: str, class_: Class, db=Depends(get_db)):
    try:
        result = await class_service.update_class(db, classId, class_)
        if result.get("message") == "Cập nhật lớp học không thành công":
            raise HTTPException(status_code=404, detail="Không tìm thấy lớp học")
        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

@router.delete("/{classId}")
async def delete_class_endpoint(classId: str, db=Depends(get_db)):
    result = await class_service.delete_class(db, classId)
    if result.get("message") == "Không tìm thấy lớp học để xóa":
        raise HTTPException(status_code=404, detail="Không tìm thấy lớp học")
    return result