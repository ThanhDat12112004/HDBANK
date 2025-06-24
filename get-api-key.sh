#!/bin/bash

# Script để lấy API key sau khi deploy
SERVICE_NAME="serverless-crud-python-demo"
STAGE=${1:-dev}  # Default stage là 'dev' nếu không truyền parameter

echo "🔑 Lấy thông tin API Key cho service: $SERVICE_NAME - stage: $STAGE"

# Lấy API Key ID từ CloudFormation stack
API_KEY_ID=$(aws cloudformation describe-stacks \
    --stack-name "$SERVICE_NAME-$STAGE" \
    --query "Stacks[0].Outputs[?OutputKey=='ApiKeyId'].OutputValue" \
    --output text)

if [ "$API_KEY_ID" = "None" ] || [ -z "$API_KEY_ID" ]; then
    echo "❌ Không tìm thấy API Key ID trong stack outputs"
    exit 1
fi

echo "📋 API Key ID: $API_KEY_ID"

# Lấy giá trị API Key
API_KEY_VALUE=$(aws apigateway get-api-key \
    --api-key "$API_KEY_ID" \
    --include-value \
    --query "value" \
    --output text)

if [ -z "$API_KEY_VALUE" ]; then
    echo "❌ Không thể lấy giá trị API Key"
    exit 1
fi

echo "✅ API Key Value: $API_KEY_VALUE"

# Lấy API Gateway URL
API_GATEWAY_ID=$(aws cloudformation describe-stacks \
    --stack-name "$SERVICE_NAME-$STAGE" \
    --query "Stacks[0].Outputs[?OutputKey=='ApiGatewayRestApiId'].OutputValue" \
    --output text)

if [ "$API_GATEWAY_ID" != "None" ] && [ -n "$API_GATEWAY_ID" ]; then
    API_URL="https://$API_GATEWAY_ID.execute-api.us-east-1.amazonaws.com/$STAGE"
    echo "🌐 API Gateway URL: $API_URL"
fi

echo ""
echo "📝 Cách sử dụng API Key:"
echo "Thêm header sau vào requests:"
echo "x-api-key: $API_KEY_VALUE"
echo ""
echo "💡 Ví dụ với curl:"
echo "curl -H \"x-api-key: $API_KEY_VALUE\" $API_URL/items"
