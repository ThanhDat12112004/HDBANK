from datetime import datetime
from pydantic import ValidationError
from app.schemas.student import Student

async def create_student(db, student: Student):
    try:
        date_of_birth = datetime.strptime(student.dateofbirth, '%Y-%m-%d').date() if student.dateofbirth else None
        query = """
            INSERT INTO Students (StudentID, FullName, DateOfBirth, Gender, Address, Email, Phone, ClassID, AccountID)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            student.studentid,
            student.fullname,
            date_of_birth,
            student.gender,
            student.address,
            student.email,
            student.phone,
            student.classid,
            student.accountid
        )
        if result:
            student = dict(result)
            if student.get('dateofbirth'):
                student['dateofbirth'] = student['dateofbirth'].strftime('%Y-%m-%d')
            return student
        return None
    except ValidationError as ve:
        raise ValueError("Dữ liệu không hợp lệ", ve.errors())
    except Exception as e:
        raise ValueError("Thêm sinh viên không thành công", str(e))

async def get_student(db, studentId: str):
    try:
        query = "SELECT * FROM Students WHERE StudentID = $1"
        result = await db.fetchrow(query, studentId)
        if result:
            student = dict(result)
            if student.get('dateofbirth'):
                student['dateofbirth'] = student['dateofbirth'].strftime('%Y-%m-%d')
            return student
        return None
    except Exception as e:
        return None

async def get_all_students(db):
    try:
        query = "SELECT * FROM Students"
        results = await db.fetch(query)
        students = [dict(result) for result in results]
        for student in students:
            if student.get('dateofbirth'):
                student['dateofbirth'] = student['dateofbirth'].strftime('%Y-%m-%d')      
        return students
    except Exception as e:
        return None

async def update_student(db, studentId: str, student: Student):
    try:
        date_of_birth = datetime.strptime(student.dateOfBirth, '%Y-%m-%d').date() if student.dateOfBirth else None
        query = """
            UPDATE Students
            SET FullName = $1, DateOfBirth = $2, Gender = $3, Address = $4, Email = $5, Phone = $6, ClassID = $7
            WHERE StudentID = $8
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            student.fullName,
            date_of_birth,
            student.gender,
            student.address,
            student.email,
            student.phone,
            student.classId,
            studentId
        )
        if result:
            return {"message": "Cập nhật sinh viên thành công", "data": dict(result)}
        return {"message": "Cập nhật sinh viên không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Cập nhật sinh viên không thành công", "error": str(e)}

async def delete_student(db, studentId: str):
    try:
        query = "DELETE FROM Students WHERE StudentID = $1 RETURNING *"
        result = await db.fetchrow(query, studentId)
        if result is not None:
            return {"message": "Xóa sinh viên thành công", "data": dict(result)}
        return {"message": "Không tìm thấy sinh viên để xóa"}
    except Exception as e:
        return {"message": "Xóa sinh viên không thành công", "error": str(e)}