import uvicorn
import os
from dotenv import load_dotenv
from fastapi import FastAPI
from src.routes.route import router  # Import router từ src.routes.route

# Load environment variables
load_dotenv()

# Khởi tạo FastAPI app (bỏ lifespan nếu không cần)
app = FastAPI(
    title="Student Service API",
    description="Manage student information",
    version="1.0.0"
)

# Gắn router vào app
app.include_router(router)

# Check required environment variables
required_vars = ["POSTGRES_URI"]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    raise Exception(f"Missing environment variables: {missing_vars}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000,workers = 1, log_level="info")