from pydantic import ValidationError
from app.schemas.grade import Grade

async def create_grade(db, grade: Grade):
    try:
        query = """
            INSERT INTO Grades (StudentID, SubjectID, Score, Semester)
            VALUES ($1, $2, $3, $4)
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            grade.studentId,
            grade.subjectId,
            grade.score,
            grade.semester
        )
        if result:
            return {"message": "Thêm điểm số thành công", "data": dict(result)}
        return {"message": "Thêm điểm số không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Thêm điểm số không thành công", "error": str(e)}

async def get_grade(db, gradeId: int):
    try:
        query = "SELECT * FROM Grades WHERE GradeID = $1"
        result = await db.fetchrow(query, gradeId)
        if result:
            return {"data": dict(result), "message": "Đã tìm thấy điểm số"}
        return {"data": None, "message": "Không tìm thấy điểm số"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi tìm điểm số", "error": str(e)}

async def get_all_grades(db):
    try:
        query = "SELECT * FROM Grades"
        results = await db.fetch(query)
        grades = [dict(result) for result in results]
        return {"data": grades, "message": "Lấy danh sách điểm số thành công"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi lấy danh sách điểm số", "error": str(e)}

async def update_grade(db, gradeId: int, grade: Grade):
    try:
        query = """
            UPDATE Grades
            SET StudentID = $1, SubjectID = $2, Score = $3, Semester = $4
            WHERE GradeID = $5
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            grade.studentId,
            grade.subjectId,
            grade.score,
            grade.semester,
            gradeId
        )
        if result:
            return {"message": "Cập nhật điểm số thành công", "data": dict(result)}
        return {"message": "Cập nhật điểm số không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Cập nhật điểm số không thành công", "error": str(e)}

async def delete_grade(db, gradeId: int):
    try:
        query = "DELETE FROM Grades WHERE GradeID = $1 RETURNING *"
        result = await db.fetchrow(query, gradeId)
        if result is not None:
            return {"message": "Xóa điểm số thành công", "data": dict(result)}
        return {"message": "Không tìm thấy điểm số để xóa"}
    except Exception as e:
        return {"message": "Xóa điểm số không thành công", "error": str(e)}