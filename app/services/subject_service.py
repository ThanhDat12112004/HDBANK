from pydantic import ValidationError
from app.schemas.subject import Subject

async def create_subject(db, subject: Subject):
    try:
        query = """
            INSERT INTO Subjects (SubjectID, SubjectName, Credits)
            VALUES ($1, $2, $3)
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            subject.subjectId,
            subject.subjectName,
            subject.credits
        )
        if result:
            return {"message": "Thêm môn học thành công", "data": dict(result)}
        return {"message": "Thêm môn học không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Thêm môn học không thành công", "error": str(e)}

async def get_subject(db, subjectId: str):
    try:
        query = "SELECT * FROM Subjects WHERE SubjectID = $1"
        result = await db.fetchrow(query, subjectId)
        if result:
            return {"data": dict(result), "message": "Đã tìm thấy môn học"}
        return {"data": None, "message": "Không tìm thấy môn học"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi tìm môn học", "error": str(e)}

async def get_all_subjects(db):
    try:
        query = "SELECT * FROM Subjects"
        results = await db.fetch(query)
        subjects = [dict(result) for result in results]
        return {"data": subjects, "message": "Lấy danh sách môn học thành công"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi lấy danh sách môn học", "error": str(e)}

async def update_subject(db, subjectId: str, subject: Subject):
    try:
        query = """
            UPDATE Subjects
            SET SubjectName = $1, Credits = $2
            WHERE SubjectID = $3
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            subject.subjectName,
            subject.credits,
            subjectId
        )
        if result:
            return {"message": "Cập nhật môn học thành công", "data": dict(result)}
        return {"message": "Cập nhật môn học không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Cập nhật môn học không thành công", "error": str(e)}

async def delete_subject(db, subjectId: str):
    try:
        query = "DELETE FROM Subjects WHERE SubjectID = $1 RETURNING *"
        result = await db.fetchrow(query, subjectId)
        if result is not None:
            return {"message": "Xóa môn học thành công", "data": dict(result)}
        return {"message": "Không tìm thấy môn học để xóa"}
    except Exception as e:
        return {"message": "Xóa môn học không thành công", "error": str(e)}