from fastapi import FastAPI, File, UploadFile, HTTPException
import pandas as pd
import io

app = FastAPI(title="Cloud Analytics Worker")

@app.post("/analyze")
async def analyze_data(file: UploadFile = File(...)):
    try:
        # Читаем содержимое файла
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        if df.empty:
            return {"error": "File is empty"}
        
        description = df.describe().to_dict()
        
        return {
            "rows": len(df),
            "columns": list(df.columns),
            "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "missing_values": df.isnull().sum().to_dict(),
            "summary": description,
            "sample": df.head(5).to_dict(orient='records')
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Analysis failed: {str(e)}")
    
@app.get("/health")
async def health():
    return {"status": "analytics service is healthy"}