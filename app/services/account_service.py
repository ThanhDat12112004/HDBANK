from pydantic import ValidationError
from app.schemas.account import AccountCreate, AccountUpdate
from app.core.security import get_password_hash, verify_password
from typing import Optional

async def create_account(db, account: AccountCreate):
    try:
        # Mã hóa mật khẩu trước khi lưu
        password_hash = get_password_hash(account.password)
        query = """
            INSERT INTO Accounts (username, passwordhash, role, created_at, updated_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING *
        """
        result = await db.fetchrow(
            query,
            account.username,
            password_hash,
            account.role
        )
        if result:
            return {"message": "Thêm tài khoản thành công", "data": dict(result)}
        return {"message": "Thêm tài khoản không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Thêm tài khoản không thành công", "error": str(e)}

async def get_account(db, accountId: int):
    try:
        query = "SELECT * FROM Accounts WHERE accountid = $1"
        result = await db.fetchrow(query, accountId)
        if result:
            return {"data": dict(result), "message": "Đã tìm thấy tài khoản"}
        return {"data": None, "message": "Không tìm thấy tài khoản"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi tìm tài khoản", "error": str(e)}

async def get_account_by_username(db, username: str):
    try:
        query = "SELECT * FROM Accounts WHERE username = $1"
        result = await db.fetchrow(query, username)
        if result:
            return {"data": dict(result), "message": "Đã tìm thấy tài khoản"}
        return {"data": None, "message": "Không tìm thấy tài khoản"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi tìm tài khoản", "error": str(e)}

async def get_all_accounts(db):
    try:
        query = "SELECT * FROM Accounts"
        results = await db.fetch(query)
        accounts = [dict(result) for result in results]
        return {"data": accounts, "message": "Lấy danh sách tài khoản thành công"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi lấy danh sách tài khoản", "error": str(e)}

async def update_account(db, accountId: int, account: AccountUpdate):
    try:
        query = """
            UPDATE Accounts
            SET username = $1, passwordhash = $2, role = $3, updated_at = CURRENT_TIMESTAMP
            WHERE accountid = $4
            RETURNING *
        """
        # Mã hóa mật khẩu nếu được cung cấp
        password_hash = get_passwordhash(account.password) if account.password else None
        result = await db.fetchrow(
            query,
            account.username,
            password_hash or None,
            account.role,
            accountId
        )
        if result:
            return {"message": "Cập nhật tài khoản thành công", "data": dict(result)}
        return {"message": "Cập nhật tài khoản không thành công"}
    except ValidationError as ve:
        return {"message": "Dữ liệu không hợp lệ", "error": str(ve)}
    except Exception as e:
        return {"message": "Cập nhật tài khoản không thành công", "error": str(e)}

async def delete_account(db, accountId: int):
    try:
        query = "DELETE FROM Accounts WHERE accountid = $1 RETURNING *"
        result = await db.fetchrow(query, accountId)
        if result is not None:
            return {"message": "Xóa tài khoản thành công", "data": dict(result)}
        return {"message": "Không tìm thấy tài khoản để xóa"}
    except Exception as e:
        return {"message": "Xóa tài khoản không thành công", "error": str(e)}

async def authenticate_account(db, username: str, password: str):
    try:
        # Tìm tài khoản theo username
        query = """
            SELECT accountid, username, passwordhash, role
            FROM Accounts
            WHERE username = $1
        """
        account = await db.fetchrow(query, username)
        if not account or not verify_password(password, account["passwordhash"]):
            return {"data": None, "message": "Tên đăng nhập hoặc mật khẩu không đúng"}
        return {"data": dict(account), "message": "Xác thực thành công"}
    except Exception as e:
        return {"data": None, "message": "Lỗi khi xác thực tài khoản", "error": str(e)}