from pydantic import ValidationError
from app.schemas.faculty import Faculty

async def create_faculty(db, faculty: Faculty):
    try:
        query = """
            INSERT INTO Faculties (FacultyID, FacultyName)
            VALUES ($1, $2)
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            faculty.facultyId,
            faculty.facultyName
        )
        if result:
            return {"message": "Thêm khoa thành công", "data": dict(result)}
        return {"message": "Thêm khoa không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Thêm khoa không thành công", "error": str(e)}

async def get_faculty(db, facultyId: str):
    try:
        query = "SELECT * FROM Faculties WHERE FacultyID = $1"
        result = await db.fetchrow(query, facultyId)
        if result:
            return {"data": dict(result), "message": "Đã tìm thấy khoa"}
        return {"data": None, "message": "Không tìm thấy khoa"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi tìm khoa", "error": str(e)}

async def get_all_faculties(db):
    try:
        query = "SELECT * FROM Faculties"
        results = await db.fetch(query)
        faculties = [dict(result) for result in results]
        return {"data": faculties, "message": "Lấy danh sách khoa thành công"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi lấy danh sách khoa", "error": str(e)}

async def update_faculty(db, facultyId: str, faculty: Faculty):
    try:
        query = """
            UPDATE Faculties
            SET FacultyName = $1
            WHERE FacultyID = $2
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            faculty.facultyName,
            facultyId
        )
        if result:
            return {"message": "Cập nhật khoa thành công", "data": dict(result)}
        return {"message": "Cập nhật khoa không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Cập nhật khoa không thành công", "error": str(e)}

async def delete_faculty(db, facultyId: str):
    try:
        query = "DELETE FROM Faculties WHERE FacultyID = $1 RETURNING *"
        result = await db.fetchrow(query, facultyId)
        if result is not None:
            return {"message": "Xóa khoa thành công", "data": dict(result)}
        return {"message": "Không tìm thấy khoa để xóa"}
    except Exception as e:
        return {"message": "Xóa khoa không thành công", "error": str(e)}