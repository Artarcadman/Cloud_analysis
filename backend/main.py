import os
import io
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from minio.error import S3Error

# Инициализация FastAPI
app = FastAPI(title="Cloud Analytics Service")

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

# Отладочный вывод (можно убрать в проде)
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
print(f"MinIO Bucket: {MINIO_BUCKET}")

# Убедитесь, что endpoint не содержит протокол
# Minio client сам добавит http:// или https://
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
    print(f"Error initializing MinIO client: {e}")
    # В продуктиве лучше поднимать исключение
    # raise HTTPException(status_code=500, detail=f"Storage connection failed: {e}")

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
                df = pd.read_csv(io.BytesIO(contents))
                
                # Базовая статистика
                description = df.describe().to_dict() if not df.empty else {}
                
                # Дополнительная информация
                stats = {
                    "filename": file.filename,
                    "rows": len(df),
                    "columns": list(df.columns),
                    "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
                    "missing_values": df.isnull().sum().to_dict(),
                    "summary": description,
                    "sample": df.head(5).to_dict(orient='records') if not df.empty else []
                }
            except Exception as e:
                stats = {
                    "error": f"Could not analyze CSV: {str(e)}",
                    "filename": file.filename
                }
        
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