FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cashe-dir -r requirements.txt

COPY app.py .

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]