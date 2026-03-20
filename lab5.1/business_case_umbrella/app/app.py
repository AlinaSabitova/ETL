import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime
 
st.set_page_config(page_title="Прогноз погоды Амстердам", layout="wide")
st.title("Анализ погоды в Амстердаме на 7 дней (Вариант 12)")
 
data_path = '/opt/airflow/data/clean_weather.csv'
original_data_path = '/opt/airflow/data/weather_forecast.csv'
 
if os.path.exists(original_data_path):
    original_df = pd.read_csv(original_data_path)
    
    st.header("Исходные данные о погоде")
    st.write("Данные, полученные из Open-Meteo API (прогноз на 7 дней)")
    st.dataframe(original_df)
    
    # Отфильтрованные данные
    filtered_df = pd.read_csv(data_path) if os.path.exists(data_path) else pd.DataFrame()
    
    st.header("Отфильтрованные данные (температура ниже 0°C)")
    
    if len(filtered_df) > 0:
        # Если есть отфильтрованные данные - показываем таблицу
        st.success(f"Найдено {len(filtered_df)} дней с температурой ниже 0°C")
        st.dataframe(filtered_df)
    else:
        # Если нет отфильтрованных данных - выводим сообщение
        st.warning("⚠️ В прогнозе нет дней с температурой ниже 0°C")
    
    # Средняя температура
    st.header("Средняя температура за период")
    
    # Используем исходные данные для расчета средней температуры
    avg_temperature = original_df['temperature'].mean()
    st.metric(label="Средняя температура", value=f"{avg_temperature:.2f} °C")
    
else:
    st.warning("""Данные еще не сгенерированы""")