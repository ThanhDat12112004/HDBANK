# Serverless CRUD Demo với Python + API Key Authentication

Demo đơn giản cho serverless API với Python, AWS Lambda và API key authentication. Không sử dụng database, chỉ lưu trữ tạm thời trong memory.

## 🚀 Tính năng CRUD + Auth

- ✅ **CREATE** - Tạo mới item (`POST /items`) - Cần quyền `write`
- ✅ **READ** - Lấy item theo ID (`GET /items/{id}`) - Cần quyền `read`
- ✅ **READ ALL** - Lấy tất cả items (`GET /items`) - Cần quyền `read`
- ✅ **UPDATE** - Cập nhật item (`PUT /items/{id}`) - Cần quyền `write`
- ✅ **DELETE** - Xóa item (`DELETE /items/{id}`) - Cần quyền `delete`
- 🔑 **GENERATE API KEY** - Tạo API key mới (`POST /generate-api-key`) - Không cần auth

## � API Keys có sẵn

```
hdbank_demo_123 - Quyền: read, write (cho demo)
hdbank_mobile_456 - Quyền: read (cho mobile app)
hdbank_admin_789 - Quyền: read, write, delete (cho admin)
```

## �📋 Cài đặt

### 1. Cài đặt dependencies
```bash
npm install
```

### 2. Deploy lên AWS
```bash
# Deploy to dev
npm run deploy:dev

# Deploy to prod
npm run deploy:prod
```

### 3. Chạy local
```bash
npm run start:local
```

## 📚 API Examples với Authentication

### Tạo API key mới (không cần auth)
```bash
curl -X POST http://localhost:3000/generate-api-key \
  -H "Content-Type: application/json" \
  -d '{
    "app_name": "my-app",
    "permissions": ["read", "write"],
    "description": "My app API key"
  }'
```

### Tạo item mới (cần API key + quyền write)
```bash
curl -X POST http://localhost:3000/items \
  -H "Content-Type: application/json" \
  -H "X-API-Key: hdbank_demo_123" \
  -d '{"name": "Laptop", "description": "Dell XPS 13", "price": 25000000}'
```

### Lấy tất cả items (cần API key + quyền read)
```bash
curl -X GET http://localhost:3000/items \
  -H "X-API-Key: hdbank_demo_123"
```

### Lấy item theo ID (cần API key + quyền read)
```bash
curl -X GET http://localhost:3000/items/{id} \
  -H "X-API-Key: hdbank_demo_123"
```

### Cập nhật item (cần API key + quyền write)
```bash
curl -X PUT http://localhost:3000/items/{id} \
  -H "Content-Type: application/json" \
  -H "X-API-Key: hdbank_demo_123" \
  -d '{"name": "Updated Laptop", "price": 30000000}'
```

### Xóa item (cần API key + quyền delete)
```bash
curl -X DELETE http://localhost:3000/items/{id} \
  -H "X-API-Key: hdbank_admin_789"
```

## 🔑 Cách truyền API Key

Có thể truyền API key qua các header sau:
```bash
# Cách 1: X-API-Key header (khuyên dùng)
-H "X-API-Key: hdbank_demo_123"

# Cách 2: Authorization header với Bearer
-H "Authorization: Bearer hdbank_demo_123"

# Cách 3: API-Key header
-H "API-Key: hdbank_demo_123"
```

## 📁 Cấu trúc

```
.
├── src/
│   ├── handlers/
│   │   ├── items.py          # CRUD handlers với auth
│   │   └── admin.py          # Generate API key
│   ├── auth.py               # API key authentication
│   ├── models.py             # Data models
│   └── utils.py              # Utilities
├── package.json              # Node dependencies
├── requirements.txt          # Python dependencies
├── serverless.yml            # Serverless config
└── README_SIMPLE.md
```

## ⚠️ Lưu ý

- Data chỉ lưu trong memory, sẽ mất khi Lambda container restart
- API keys cũng lưu trong memory, chỉ dùng cho demo
- Trong production nên lưu API keys trong database
- Mỗi function riêng biệt nên data không share giữa các functions
- Cần API key cho tất cả endpoints trừ `/generate-api-key`

## 🚨 Error Codes

- `401 Unauthorized` - Thiếu hoặc sai API key
- `403 Forbidden` - Không có quyền thực hiện action
- `400 Bad Request` - Dữ liệu không hợp lệ
- `404 Not Found` - Item không tồn tại
- `500 Internal Server Error` - Lỗi server
