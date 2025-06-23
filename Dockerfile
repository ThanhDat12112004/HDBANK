# Sử dụng image Python chính thức
FROM python:3.11-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Cài đặt các công cụ cần thiết cho PostgreSQL và build
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Sao chép file requirements.txt và cài đặt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã nguồn
COPY . .

# Mở cổng từ biến môi trường SERVER_PORT (mặc định 3000)
EXPOSE ${SERVER_PORT:-3000}

# Chạy FastAPI server với uvicorn, sử dụng biến môi trường
CMD ["python", "main.py"]