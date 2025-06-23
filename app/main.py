from fastapi import FastAPI
from app.core.config import settings
from app.api.v1.endpoints import student, teacher, class_, faculty, grade, subject, auth
from app.db.init_db import init_db
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup event
    logger.info("Initializing database...")
    await init_db()
    logger.info("Database initialized successfully")
    yield
    # Shutdown event (if needed)
    logger.info("Shutting down application...")


app = FastAPI(
    lifespan=lifespan,
    title=settings.PROJECT_NAME,
    description="Manage student information",
    version="1.0.0",
    # redirect_slashes=False
)



@app.get("/")
def read_root():
    return {"message": "API quản lý sinh viên!"}

# Include authentication router
app.include_router(auth.router, prefix=settings.API_V1_STR)
# Include other routers with prefix
app.include_router(student.router, prefix=settings.API_V1_STR)
app.include_router(teacher.router, prefix=settings.API_V1_STR)
app.include_router(class_.router, prefix=settings.API_V1_STR)
app.include_router(faculty.router, prefix=settings.API_V1_STR)
app.include_router(grade.router, prefix=settings.API_V1_STR)
app.include_router(subject.router, prefix=settings.API_V1_STR)