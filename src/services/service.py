async def create_student_db(db, student):
    try:
        query = "INSERT INTO students (hovaten, tuoi) VALUES ($1, $2) RETURNING *"
        result = await db.fetchrow(query, student.hovaten, student.tuoi)
        if result:
            return {"message": "Thêm sinh viên thành công"}
        else:
            return {"message": "Thêm sinh viên không thành công"}
    except Exception as e:
        return {"message": "Thêm sinh viên không thành công"}

async def get_student_db(db, mssv: int):
    try:
        query = "SELECT * FROM students WHERE mssv = $1"
        result = await db.fetchrow(query, mssv)
        return {"data": dict(result), "message": "Đã tìm thấy sinh viên"}
    except Exception as e:
        return {"data": None, "message": "Không tìm thấy sinh viên"}

async def get_all_students_db(db):
    try:
        query = "SELECT * FROM students"
        results = await db.fetch(query)
        return {"data": [dict(result) for result in results], "message": "Lấy danh sách sinh viên thành công"}
    except Exception as e:
        return {"data": None, "message": "Không tìm thấy sinh viên"}
async def update_student_db(db, mssv: int, student):
    query = """
    UPDATE students
    SET hovaten = $1, tuoi = $2
    WHERE mssv = $3
    returning *;
    """
    result = await db.fetchrow(query, student.hovaten, student.tuoi, mssv)
    if result:
        return {"message": "Cập nhật sinh viên thành công"}
    else:
        return {"message": "Cập nhật sinh viên không thành công"}

async def delete_student_db(db, mssv: int):
    try:
        query = "DELETE FROM students WHERE mssv = $1 returning *"
        result = await db.fetchrow(query, mssv)
        if result is not None:
            return {"message": "Xóa sinh viên thành công"}
        else:
            return {"message": "Không tìm thấy sinh viên để xóa"}
    except Exception as e:
        return {"message": "Xóa sinh viên không thành công"}