import os
import io
import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from minio.error import S3Error

app = FastAPI(title="Cloud Gateway Backend")


# Добавляем CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене заменить на конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Получаем переменные окружения с значениями по умолчанию
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datasets")

ANALYTICS_URL = os.getenv("ANALYTICS_URL", "http://analytics:8001")

# Отладочный вывод
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
print(f"MinIO Bucket: {MINIO_BUCKET}")


if MINIO_ENDPOINT and MINIO_ENDPOINT.startswith("http://"):
    MINIO_ENDPOINT = MINIO_ENDPOINT.replace("http://", "")
elif MINIO_ENDPOINT and MINIO_ENDPOINT.startswith("https://"):
    MINIO_ENDPOINT = MINIO_ENDPOINT.replace("https://", "")

# Инициализация MinIO клиента
try:
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Для локальной разработки без HTTPS
    )
    
    # Проверяем существование бакета и создаем если нужно
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' created successfully.")
    else:
        print(f"Bucket '{MINIO_BUCKET}' already exists.")
        
    print("MinIO client initialized successfully.")
    
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Storage connection failed: {e}")

# Маршруты API
@app.get("/")
async def root():
    return {
        "service": "Cloud Analytics Backend",
        "status": "running",
        "minio_bucket": MINIO_BUCKET,
        "endpoint": MINIO_ENDPOINT
    }

@app.get("/health")
async def health_check():
    try:
        # Проверяем соединение с MinIO
        buckets = client.list_buckets()
        bucket_names = [bucket.name for bucket in buckets]
        
        return {
            "status": "healthy",
            "service": "Cloud Analytics Backend",
            "minio_connected": True,
            "available_buckets": bucket_names,
            "bucket": MINIO_BUCKET
        }
    except Exception as e:
        return {
            "status": "degraded",
            "service": "Cloud Analytics Backend",
            "minio_connected": False,
            "error": str(e)
        }

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="No filename provided")
        
        # Читаем файл в память
        contents = await file.read()
        file_size = len(contents)
        
        print(f"Uploading file: {file.filename}, size: {file_size} bytes")
        
        # Сохраняем в MinIO
        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=file.filename,
            data=io.BytesIO(contents),
            length=file_size,
            content_type=file.content_type or "application/octet-stream"
        )
        
        # Анализ данных (только для CSV)
        stats = {}
        if file.filename.lower().endswith('.csv'):
            try:
                async with httpx.AsyncClient() as httpx_client:
                
                    # Пересылаем файл в микросервис аналитики
                    files = {'file': (file.filename, contents, file.content_type)}
                    response = await httpx_client.post(f"{ANALYTICS_URL}/analyze", files=files, timeout=10.0)
                
                    if response.status_code == 200:
                        stats = response.json()
                    else:
                        stats = {"error": f"Analytics service error: {response.text}"}
            except Exception as e:
                stats = {"error": f"Could not connect to analytics: {str(e)}"}
        
        return {
            "filename": file.filename,
            "status": "uploaded",
            "storage": "MinIO S3",
            "bucket": MINIO_BUCKET,
            "size_bytes": file_size,
            "analytics": stats,
            "message": f"File '{file.filename}' uploaded successfully to bucket '{MINIO_BUCKET}'"
        }
        
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"Storage error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/files")
async def list_files():
    try:
        objects = client.list_objects(MINIO_BUCKET, recursive=True)
        files = []
        
        for obj in objects:
            files.append({
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                "etag": obj.etag
            })
        
        return {
            "bucket": MINIO_BUCKET,
            "file_count": len(files),
            "files": files
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")

@app.get("/files/{filename}")
async def get_file_info(filename: str):
    try:
        stat = client.stat_object(MINIO_BUCKET, filename)
        
        return {
            "filename": filename,
            "size": stat.size,
            "content_type": stat.content_type,
            "last_modified": stat.last_modified.isoformat() if stat.last_modified else None,
            "etag": stat.etag,
            "metadata": stat.metadata
        }
    except S3Error as e:
        if e.code == "NoSuchKey":
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.delete("/files/{filename}")
async def delete_file(filename: str):
    try:
        client.remove_object(MINIO_BUCKET, filename)
        return {
            "filename": filename,
            "status": "deleted",
            "message": f"File '{filename}' deleted successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")