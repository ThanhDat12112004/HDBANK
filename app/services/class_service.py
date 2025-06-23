from pydantic import ValidationError
from app.schemas.class_ import Class

async def create_class(db, class_: Class):
    try:
        query = """
            INSERT INTO Classes (ClassID, ClassName, TeacherID, FacultyID)
            VALUES ($1, $2, $3, $4)
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            class_.classId,
            class_.className,
            class_.teacherId,
            class_.facultyId
        )
        if result:
            return {"message": "Thêm lớp học thành công", "data": dict(result)}
        return {"message": "Thêm lớp học không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Thêm lớp học không thành công", "error": str(e)}

async def get_class(db, classId: str):
    try:
        query = "SELECT * FROM Classes WHERE ClassID = $1"
        result = await db.fetchrow(query, classId)
        if result:
            return result
        return {"data": None, "message": "Không tìm thấy lớp học"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi tìm lớp học", "error": str(e)}

async def get_all_classes(db):
    try:
        query = "SELECT * FROM Classes"
        results = await db.fetch(query)
        classes = [dict(result) for result in results]
        return results
    except Exception as e:
        return {"data": None, "message": "Lỗi khi lấy danh sách lớp học", "error": str(e)}

async def update_class(db, classId: str, class_: Class):
    try:
        query = """
            UPDATE Classes
            SET ClassName = $1, TeacherID = $2, FacultyID = $3
            WHERE ClassID = $4
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            class_.className,
            class_.teacherId,
            class_.facultyId,
            classId
        )

        return result 
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Cập nhật lớp học không thành công", "error": str(e)}
async def delete_class(db, classId: str):
    try:
        query = "DELETE FROM Classes WHERE ClassID = $1 RETURNING *"
        result = await db.fetchrow(query, classId)
        if result is not None:
            return {"message": "Xóa lớp học thành công", "data": dict(result)}
        return {"message": "Không tìm thấy lớp học để xóa"}
    except Exception as e:
        return {"message": "Xóa lớp học không thành công", "error": str(e)}