import json
import uuid
from datetime import datetime
from typing import Dict, Any
import logging

# Thiết lập logging để debug
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory storage với sản phẩm mẫu
ITEMS_STORAGE: Dict[str, Dict[str, Any]] = {
    "sample-item-1": {
        "id": "sample-item-1",
        "name": "Sản phẩm mẫu 1",
        "description": "Mô tả sản phẩm mẫu 1",
        "price": 100,
        "category": "test",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    },
    "sample-item-2": {
        "id": "sample-item-2",
        "name": "Sản phẩm mẫu 2",
        "description": "Mô tả sản phẩm mẫu 2",
        "price": 200,
        "category": "general",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
}

def create_response(status_code: int, body: Any) -> Dict[str, Any]:
    """Tạo response chuẩn cho API Gateway"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-API-Key',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE'
        },
        'body': json.dumps(body, default=str)
    }

def create(event, context):
    """CREATE - Tạo item mới"""
    try:
        body = json.loads(event.get('body', '{}'))
        if not body.get('name'):
            return create_response(400, {'error': 'Thiếu trường bắt buộc: name'})
        item_id = str(uuid.uuid4())
        item = {
            'id': item_id,
            'name': body['name'],
            'description': body.get('description', ''),
            'price': body.get('price', 0),
            'category': body.get('category', 'general'),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        ITEMS_STORAGE[item_id] = item
        logger.info(f"Created item: {item_id}")
        return create_response(201, {
            'message': 'Item đã được tạo thành công',
            'item': item
        })
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON format'})
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, {'error': f'Internal server error: {str(e)}'})

def get(event, context):
    """READ - Lấy item theo ID"""
    try:
        item_id = event['pathParameters']['id']
        if item_id not in ITEMS_STORAGE:
            return create_response(404, {'error': 'Item không tồn tại'})
        item = ITEMS_STORAGE[item_id]
        logger.info(f"Retrieved item: {item_id}")
        return create_response(200, {'item': item})
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, {'error': f'Internal server error: {str(e)}'})

def list_items(event, context):
    """READ - Lấy danh sách tất cả items"""
    try:
        logger.info(f"Event: {event}")
        query_params = event.get('queryStringParameters') or {}
        limit = int(query_params.get('limit', 100))
        category = query_params.get('category')
        items = list(ITEMS_STORAGE.values())
        if category:
            items = [item for item in items if item.get('category') == category]
        items = items[:limit]
        items.sort(key=lambda x: x['created_at'], reverse=True)
        logger.info(f"Retrieved {len(items)} items")
        return create_response(200, {
            'items': items,
            'total': len(items),
            'message': f'Tìm thấy {len(items)} items'
        })
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, {'error': f'Internal server error: {str(e)}'})

def update(event, context):
    """UPDATE - Cập nhật item"""
    try:
        item_id = event['pathParameters']['id']
        if item_id not in ITEMS_STORAGE:
            return create_response(404, {'error': 'Item không tồn tại'})
        body = json.loads(event.get('body', '{}'))
        current_item = ITEMS_STORAGE[item_id]
        if 'name' in body:
            current_item['name'] = body['name']
        if 'description' in body:
            current_item['description'] = body['description']
        if 'price' in body:
            current_item['price'] = body['price']
        if 'category' in body:
            current_item['category'] = body['category']
        current_item['updated_at'] = datetime.now().isoformat()
        ITEMS_STORAGE[item_id] = current_item
        logger.info(f"Updated item: {item_id}")
        return create_response(200, {
            'message': 'Item đã được cập nhật thành công',
            'item': current_item
        })
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON format'})
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, {'error': f'Internal server error: {str(e)}'})

def delete(event, context):
    """DELETE - Xóa item"""
    try:
        item_id = event['pathParameters']['id']
        if item_id not in ITEMS_STORAGE:
            return create_response(404, {'error': 'Item không tồn tại'})
        deleted_item = ITEMS_STORAGE[item_id]
        del ITEMS_STORAGE[item_id]
        logger.info(f"Deleted item: {item_id}")
        return create_response(200, {
            'message': 'Item đã được xóa thành công',
            'deleted_item': deleted_item
        })
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, {'error': f'Internal server error: {str(e)}'})
