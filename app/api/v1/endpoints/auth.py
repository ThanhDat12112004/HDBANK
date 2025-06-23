from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from app.core.security import create_access_token
from app.db.session import get_db
from app.services.account_service import authenticate_account, create_account
from app.schemas.account import AccountCreate

router = APIRouter(tags=["auth"])

@router.post("/login")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db=Depends(get_db)
):
    # Xác thực tài khoản bằng service
    auth_result = await authenticate_account(db, form_data.username, form_data.password)
    if not auth_result["data"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=auth_result["message"],
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Tạo JWT access token
    account = auth_result["data"]
    access_token = create_access_token(
        data={"sub": account["username"], "role": account["role"], "accountid": account["accountid"]}
    )
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/register")
async def register(
    acount: AccountCreate,
    db=Depends(get_db)
):
    # Kiểm tra username đã tồn tại
    existing_account = await authenticate_account(db, acount.username, acount.password)
    if existing_account["data"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Tên đăng nhập đã tồn tại",
        )

    # Tạo tài khoản mới
    account_create = AccountCreate(
        username=acount.username,
        password=acount.password,
        role='user',  
    )
    result = await create_account(db, account_create)
    if not result["data"]:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=result["message"],
        )

    # Tạo JWT access token
    account = result["data"]
    access_token = create_access_token(
        data={"sub": account["username"], "role": account["role"], "accountid": account["accountid"]}
    )
    return {"access_token": access_token, "token_type": "bearer"}