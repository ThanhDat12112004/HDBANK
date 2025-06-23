from pydantic import ValidationError
from app.schemas.teacher import Teacher

async def create_teacher(db, teacher: Teacher):
    try:
        query = """
            INSERT INTO Teachers (TeacherID, FullName, Email, Phone)
            VALUES ($1, $2, $3, $4)
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            teacher.teacherId,
            teacher.fullName,
            teacher.email,
            teacher.phone
        )
        if result:
            return {"message": "Thêm giáo viên thành công", "data": dict(result)}
        return {"message": "Thêm giáo viên không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Thêm giáo viên không thành công", "error": str(e)}

async def get_teacher(db, teacherId: str):
    try:
        query = "SELECT * FROM Teachers WHERE TeacherID = $1"
        result = await db.fetchrow(query, teacherId)
        if result:
            return {"data": dict(result), "message": "Đã tìm thấy giáo viên"}
        return {"data": None, "message": "Không tìm thấy giáo viên"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi tìm giáo viên", "error": str(e)}

async def get_all_teachers(db):
    try:
        query = "SELECT * FROM Teachers"
        results = await db.fetch(query)
        teachers = [dict(result) for result in results]
        return {"data": teachers, "message": "Lấy danh sách giáo viên thành công"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi lấy danh sách giáo viên", "error": str(e)}

async def update_teacher(db, teacherId: str, teacher: Teacher):
    try:
        query = """
            UPDATE Teachers
            SET FullName = $1, Email = $2, Phone = $3
            WHERE TeacherID = $4
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            teacher.fullName,
            teacher.email,
            teacher.phone,
            teacherId
        )
        if result:
            return {"message": "Cập nhật giáo viên thành công", "data": dict(result)}
        return {"message": "Cập nhật giáo viên không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Cập nhật giáo viên không thành công", "error": str(e)}

async def delete_teacher(db, teacherId: str):
    try:
        query = "DELETE FROM Teachers WHERE TeacherID = $1 RETURNING *"
        result = await db.fetchrow(query, teacherId)
        if result is not None:
            return {"message": "Xóa giáo viên thành công", "data": dict(result)}
        return {"message": "Không tìm thấy giáo viên để xóa"}
    except Exception as e:
        return {"message": "Xóa giáo viên không thành công", "error": str(e)}