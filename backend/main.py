import os
import io
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException
from minio import Minio
from minio.error import S3Error

app = FastAPI(title="Cloud Analytics Service")
