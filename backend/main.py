import os
import io
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException
from minio import Minio
from minio.error import S3Error

app = FastAPI(title="Cloud Analytics Service")

MINIO_ENDPOINT = os.getenv("MINIO_INTERNAL_ENDPOINT")
ACCESS_KEY = os.getenv("MINIO_ROOT_KEY")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_DEFAULT_BUCKET")



try:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False # No HTTPS 
    )
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created.")
        
except S3Error as e:
    print(f"MiniO connection error: {e}")
    
@app.get("/health")
def heath_check():
    return {"status": "running", "service": "Cloud Analytics Backend"}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        
        # Reading file in memory
        contents = await file.read()
        file_size = len(contents)
        
        # Saving to MinIO
        client.put_object(
            BUCKET_NAME,
            file.filename,
            io.BytesIO(contents),
            file_size,
            content_type=file.content_type
        )
        
        # Data Analysis
        stats = {}
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(contents))
            description = df.describe().to_dict()
            stats = {
                "rows": len(df),
                "columns": list(df.columns),
                "summary": description
            }
            
        return {
            "filename": file.filename,
            "status": "uploaded",
            "storage": "MinIO S3",
            "analytics": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
