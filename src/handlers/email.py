import json
import boto3
import logging
from datetime import datetime
from typing import Dict, Any
from botocore.exceptions import ClientError

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Khởi tạo SES client
ses_client = boto3.client('ses')

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

def send_email(event, context):
    """Gửi email sử dụng AWS SES"""
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))

        # Validate required fields
        required_fields = ['to_email', 'subject', 'message']
        for field in required_fields:
            if not body.get(field):
                return create_response(400, {
                    'error': f'Thiếu trường bắt buộc: {field}'
                })

        to_email = body['to_email']
        subject = body['subject']
        message = body['message']
        from_email = body.get('from_email', 'noreply@yourdomain.com')
        reply_to = body.get('reply_to', from_email)

        # Pre-process the message to replace newlines
        formatted_message = message.replace('\n', '<br>')

        # Tạo email content
        html_message = f"""
        <html>
            <body>
                <h2>{subject}</h2>
                <div style="font-family: Arial, sans-serif; line-height: 1.6;">
                    {formatted_message}
                </div>
                <hr>
                <p style="color: #666; font-size: 12px;">
                    Sent at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </p>
            </body>
        </html>
        """

        # Gửi email qua SES
        response = ses_client.send_email(
            Source=from_email,
            Destination={
                'ToAddresses': [to_email],
                'CcAddresses': body.get('cc_emails', []),
                'BccAddresses': body.get('bcc_emails', [])
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': message,
                        'Charset': 'UTF-8'
                    },
                    'Html': {
                        'Data': html_message,
                        'Charset': 'UTF-8'
                    }
                }
            },
            ReplyToAddresses=[reply_to]
        )

        logger.info(f"Email sent successfully. MessageId: {response['MessageId']}")

        return create_response(200, {
            'message': 'Email đã được gửi thành công',
            'message_id': response['MessageId'],
            'to_email': to_email,
            'subject': subject,
            'sent_at': datetime.now().isoformat()
        })

    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON format'})

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']

        logger.error(f"SES Error - Code: {error_code}, Message: {error_message}")

        if error_code == 'MessageRejected':
            return create_response(400, {
                'error': 'Email bị từ chối. Kiểm tra địa chỉ email và nội dung.',
                'details': error_message
            })
        elif error_code == 'MailFromDomainNotVerifiedException':
            return create_response(400, {
                'error': 'Domain gửi email chưa được verify trong SES.',
                'details': error_message
            })
        elif error_code == 'ConfigurationSetDoesNotExistException':
            return create_response(400, {
                'error': 'Configuration set không tồn tại.',
                'details': error_message
            })
        else:
            return create_response(500, {
                'error': 'Lỗi khi gửi email',
                'details': error_message
            })

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return create_response(500, {
            'error': f'Internal server error: {str(e)}'
        })

def send_template_email(event, context):
    """Gửi email sử dụng template có sẵn trong SES"""
    try:
        body = json.loads(event.get('body', '{}'))

        # Validate required fields
        required_fields = ['to_email', 'template_name']
        for field in required_fields:
            if not body.get(field):
                return create_response(400, {
                    'error': f'Thiếu trường bắt buộc: {field}'
                })

        to_email = body['to_email']
        template_name = body['template_name']
        template_data = body.get('template_data', {})
        from_email = body.get('from_email', 'noreply@yourdomain.com')

        # Gửi email với template
        response = ses_client.send_templated_email(
            Source=from_email,
            Destination={
                'ToAddresses': [to_email]
            },
            Template=template_name,
            TemplateData=json.dumps(template_data)
        )

        logger.info(f"Template email sent successfully. MessageId: {response['MessageId']}")

        return create_response(200, {
            'message': 'Template email đã được gửi thành công',
            'message_id': response['MessageId'],
            'to_email': to_email,
            'template_name': template_name,
            'sent_at': datetime.now().isoformat()
        })

    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON format'})

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']

        logger.error(f"SES Template Error - Code: {error_code}, Message: {error_message}")

        return create_response(500, {
            'error': 'Lỗi khi gửi template email',
            'details': error_message
        })

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return create_response(500, {
            'error': f'Internal server error: {str(e)}'
        })

def get_send_quota(event, context):
    """Lấy thông tin quota gửi email của SES"""
    try:
        response = ses_client.get_send_quota()

        return create_response(200, {
            'send_quota': {
                'max_24_hour': response['Max24HourSend'],
                'max_send_rate': response['MaxSendRate'],
                'sent_last_24_hours': response['SentLast24Hours']
            }
        })

    except Exception as e:
        logger.error(f"Error getting send quota: {str(e)}")
        return create_response(500, {
            'error': f'Internal server error: {str(e)}'
        })

def get_send_statistics(event, context):
    """Lấy thống kê gửi email"""
    try:
        response = ses_client.get_send_statistics()

        return create_response(200, {
            'send_statistics': response['SendDataPoints']
        })

    except Exception as e:
        logger.error(f"Error getting send statistics: {str(e)}")
        return create_response(500, {
            'error': f'Internal server error: {str(e)}'
        })
