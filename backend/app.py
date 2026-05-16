import os
import logging
import json
from fastapi import FastAPI, UploadFile, File, HTTPException, Header
from fastapi.responses import JSONResponse, StreamingResponse
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import uuid
from typing import Optional
import httpx
from kafka import KafkaProducer
import asyncio
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="File Storage Microservice", version="1.0.0")

# Конфигурация S3 из переменных окружения
S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://localhost:9000')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'minioadmin')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'minioadmin123')
S3_BUCKET = os.getenv('S3_BUCKET', 'microservice-files')

# Инициализация S3 клиента
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# User Service URL (для проверки токена)
USER_SERVICE_URL = os.getenv('USER_SERVICE_URL', 'http://user-service:8002')

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

# Kafka Producer
kafka_producer = None

def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Kafka producer connected")
        except Exception as e:
            logger.warning(f"Kafka producer not available: {e}")
    return kafka_producer

async def verify_token(token: str) -> dict:
    """Проверка JWT токена через User Service"""
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(
            f"{USER_SERVICE_URL}/users/me",
            headers={"Authorization": f"Bearer {token}"}
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=401, detail="Invalid or expired token")



@app.on_event("startup")
async def startup_event():
    """Создание bucket при старте, если его нет"""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"Bucket '{S3_BUCKET}' already exists")
    except ClientError:
        try:
            s3_client.create_bucket(Bucket=S3_BUCKET)
            logger.info(f"Bucket '{S3_BUCKET}' created successfully")
        except Exception as e:
            logger.error(f"Failed to create bucket: {e}")

@app.get("/health")
async def health_check():
    """Проверка здоровья микросервиса"""
    return {"status": "healthy", "service": "file-storage", "bucket": S3_BUCKET}

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    authorization: str = Header(None)
):
    """Загрузка файла в S3 (требуется токен)"""
    
    # 1. Проверка токена (читаем из заголовка Authorization)
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")
    
    token = authorization.replace("Bearer ", "")
    user_info = await verify_token(token)
    
    try:
        # 2. Генерация уникального имени файла
        file_extension = os.path.splitext(file.filename)[1]
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        
        # 3. Загрузка в S3
        s3_client.upload_fileobj(
            file.file,
            S3_BUCKET,
            unique_filename,
            ExtraArgs={'Metadata': {
                'original_filename': file.filename,
                'uploaded_by': user_info.get('username', 'unknown')
            }}
        )
        
        logger.info(f"File uploaded: {unique_filename} by {user_info.get('username')}")
        
        # 4. Отправка события в Kafka (асинхронно)
        kafka_producer = get_kafka_producer()
        if kafka_producer:
            try:
                kafka_producer.send('file.uploaded', {
                    'file_id': unique_filename,
                    'original_filename': file.filename,
                    'size_bytes': file.size,
                    'uploaded_by': user_info.get('username'),
                    'user_id': user_info.get('id'),
                    'timestamp': datetime.utcnow().isoformat()
                })
                kafka_producer.flush()
                logger.info(f"Event sent to Kafka: {unique_filename}")
            except Exception as e:
                logger.warning(f"Failed to send Kafka event: {e}")
        
        return JSONResponse(
            status_code=201,
            content={
                "message": "File uploaded successfully",
                "file_id": unique_filename,
                "original_filename": file.filename,
                "uploaded_by": user_info.get('username'),
                "bucket": S3_BUCKET
            }
        )
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/download/{file_id}")
async def download_file(file_id: str):
    """Скачивание файла из S3"""
    try:
        # Получение объекта из S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_id)
        
        # Получение оригинального имени из метаданных
        original_filename = response.get('Metadata', {}).get('original_filename', file_id)
        
        # Возврат файла потоком
        return StreamingResponse(
            response['Body'],
            media_type='application/octet-stream',
            headers={
                'Content-Disposition': f'attachment; filename="{original_filename}"',
                'X-File-Id': file_id
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise HTTPException(status_code=404, detail=f"File '{file_id}' not found")
        else:
            logger.error(f"Download failed: {e}")
            raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

@app.delete("/delete/{file_id}")
async def delete_file(file_id: str):
    """Удаление файла из S3"""
    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=file_id)
        logger.info(f"File deleted: {file_id}")
        return {"message": f"File '{file_id}' deleted successfully"}
    except Exception as e:
        logger.error(f"Delete failed: {e}")
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")

@app.get("/list")
async def list_files(prefix: Optional[str] = None, max_keys: int = 100):
    """Список файлов в S3 bucket"""
    try:
        kwargs = {'Bucket': S3_BUCKET, 'MaxKeys': max_keys}
        if prefix:
            kwargs['Prefix'] = prefix
        
        response = s3_client.list_objects_v2(**kwargs)
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append({
                    'file_id': obj['Key'],
                    'size_bytes': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                })
        
        return {
            "count": len(files),
            "files": files,
            "bucket": S3_BUCKET,
            "has_more": response.get('IsTruncated', False)
        }
    except Exception as e:
        logger.error(f"List failed: {e}")
        raise HTTPException(status_code=500, detail=f"List failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
