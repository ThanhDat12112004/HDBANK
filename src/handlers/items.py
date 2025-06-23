import json
import logging
from datetime import datetime
from typing import Dict, Any

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        'body': json.dumps(body, default=str, ensure_ascii=False)
    }

def create(event, context):
    """Tạo item mới (mock)"""
    try:
        body = json.loads(event.get('body', '{}'))
        required_fields = ['name', 'description']
        for field in required_fields:
            if not body.get(field):
                return create_response(400, {
                    'error': f'Thiếu trường bắt buộc: {field}'
                })

        item = {
            'id': str(datetime.now().timestamp()),  # Mock ID
            'name': body['name'],
            'description': body['description'],
            'created_at': datetime.now().isoformat()
        }

        logger.info(f"Created item with id: {item['id']}")
        return create_response(201, {
            'message': 'Tạo item thành công',
            'item': item
        })

    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON format'})
    except Exception as e:
        return create_response(500, {'error': f'Internal server error: {str(e)}'})

def get(event, context):
    """Lấy item theo ID (mock)"""
    try:
        item_id = event['pathParameters'].get('id')
        if not item_id:
            return create_response(400, {'error': 'Thiếu ID'})

        item = {
            'id': item_id,
            'name': f"Item {item_id}",
            'description': f"Mô tả item {item_id}",
            'created_at': datetime.now().isoformat()
        }

        logger.info(f"Retrieved item with id: {item_id}")
        return create_response(200, {
            'message': 'Lấy item thành công',
            'item': item
        })

    except Exception as e:
        return create_response(500, {'error': f'Internal server error: {str(e)}'})

def list_items(event, context):
    """Trả về danh sách items tĩnh (không dùng DB)"""
    try:
        # Mock data for items
        items = [
            {"id": "1", "name": "Item 1", "description": "Mô tả item 1", "created_at": datetime.now().isoformat()},
            {"id": "2", "name": "Item 2", "description": "Mô tả item 2", "created_at": datetime.now().isoformat()},
            {"id": "3", "name": "Item 3", "description": "Mô tả item 3", "created_at": datetime.now().isoformat()}
        ]

        logger.info(f"Retrieved {len(items)} items")
        return create_response(200, {
            'message': 'Lấy danh sách items thành công',
            'items': items,
            'count': len(items)
        })

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return create_response(500, {
            'error': f'Internal server error: {str(e)}'
        })

def update(event, context):
    """Cập nhật item (mock)"""
    try:
        item_id = event['pathParameters'].get('id')
        body = json.loads(event.get('body', '{}'))
        if not item_id:
            return create_response(400, {'error': 'Thiếu ID'})

        item = {
            'id': item_id,
            'name': body.get('name', f"Item {item_id}"),
            'description': body.get('description', f"Mô tả item {item_id}"),
            'updated_at': datetime.now().isoformat()
        }

        logger.info(f"Updated item with id: {item_id}")
        return create_response(200, {
            'message': 'Cập nhật item thành công',
            'item': item
        })

    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON format'})
    except Exception as e:
        return create_response(500, {'error': f'Internal server error: {str(e)}'})

def delete(event, context):
    """Xóa item (mock)"""
    try:
        item_id = event['pathParameters'].get('id')
        if not item_id:
            return create_response(400, {'error': 'Thiếu ID'})

        logger.info(f"Deleted item with id: {item_id}")
        return create_response(200, {
            'message': f'Xóa item {item_id} thành công'
        })

    except Exception as e:
        return create_response(500, {'error': f'Internal server error: {str(e)}'})
