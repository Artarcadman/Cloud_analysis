import streamlit as st
import requests
import os
import pandas as pd

BACKEND_URL = os.getenv("BACKEND_URL")

st.set_page_config(page_title="Cloud Analytics Hub", layout='wide')

st.title("Cloud Data Analytics Platform")
st.markdown("Сервис загрузки и анализа данных (S3-backend)")

with st.sidebar:
    st.header("Статус системы")
    try:
        res = requests.get(f"{BACKEND_URL}/health", timeout=2)
        if res.status_code == 200:
            st.success("Backend: Online")
        else:
            st.error("Backend: Error")
    except:
        st.error("Backend: Offline")
        
        
uploaded_file = st.file_uploader("Выберите CSV файл", type=["csv"])

if uploaded_file is not None:
    if st.button("Проанализировать"):
        with st.spinner("Обработка данных в облаке ..."):
            try:
                # Отправка файла на API
                files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "text/csv")}
                response = requests.post(f"{BACKEND_URL}/upload", files=files)
                
                if response.status_code == 200:
                    result = response.json()
                    
                    st.success(f"Файл **{result['filename']}** успешно сохранен в MinIO!")
                    
                    # Визуализация аналитики
                    st.subheader("Аналитический Отчет")
                    stats = result.get("analytics", {})
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Количество строк", stats.get("rows", 0))
                    with col2:
                        st.metric("Количество колонок", len(stats.get("columns", [])))
                        
                    st.write("Статистическое описание:")
                    st.dataframe(pd.DataFrame(stats.get("summary", {})))
                    
                else:
                    st.error(f"Ошибка сервера: {response.text}")
                    
            except Exception as e:
                st.error(f"Ошибка соединения: {e}")
                