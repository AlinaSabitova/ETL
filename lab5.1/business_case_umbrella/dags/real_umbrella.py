import os
import requests
import pandas as pd
import joblib
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sklearn.linear_model import LinearRegression
 
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}
 
dag = DAG(
    dag_id="real_umbrella_amsterdam",
    default_args=default_args,
    description="Fetch Amsterdam weather, clean, filter (<0°C), train ML, deploy.",
    schedule_interval="@daily",
    catchup=False
)
 
def fetch_weather_forecast():
    # Open-Meteo API для Амстердама (широта: 52.37, долгота: 4.89)
    url = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=52.37&longitude=4.89"
        "&daily=temperature_2m_mean"
        "&timezone=Europe%2FAmsterdam"
        "&forecast_days=7"
    )
    
    response = requests.get(url)
    data = response.json()
    
    # Извлекаем списки дат и температур из JSON
    dates = data['daily']['time']
    temperatures = data['daily']['temperature_2m_mean']
    
    # Создаем DataFrame
    df = pd.DataFrame({
        'date': dates,
        'temperature': temperatures
    })
    
    data_dir = '/opt/airflow/data'
    os.makedirs(data_dir, exist_ok=True)
    df.to_csv(os.path.join(data_dir, 'weather_forecast.csv'), index=False)
    print("Weather forecast for Amsterdam saved via Open-Meteo.")
 
def clean_weather_data():
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    
    # Очистка
    df['temperature'] = df['temperature'].ffill()
    
    # Фильтрация: оставляем только дни с температурой < 0°C
    df_filtered = df[df['temperature'] < 0].copy()
    
    # Сохраняем отфильтрованные данные
    df_filtered.to_csv(os.path.join(data_dir, 'clean_weather.csv'), index=False)
    print(f"Cleaned weather data filtered: {len(df_filtered)} days with temperature < 0°C")
 
def fetch_sales_data():
    data_dir = '/opt/airflow/data'
    # Берём даты из прогноза
    weather_df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    dates = weather_df['date'].tolist()
    
    # Моковые данные продаж
    sales = [8, 15, 10, 10, 9, 20, 30][:len(dates)]
    
    df = pd.DataFrame({'date': dates, 'sales': sales})
    df.to_csv(os.path.join(data_dir, 'sales_data.csv'), index=False)
    print("Sales data saved.")
 
def clean_sales_data():
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'sales_data.csv'))
    df['sales'] = df['sales'].ffill()
    df.to_csv(os.path.join(data_dir, 'clean_sales.csv'), index=False)
    print("Cleaned sales data saved.")
 
def join_datasets():
    data_dir = '/opt/airflow/data'
    weather_df = pd.read_csv(os.path.join(data_dir, 'clean_weather.csv'))
    sales_df = pd.read_csv(os.path.join(data_dir, 'clean_sales.csv'))
    
    # Объединение
    joined_df = pd.merge(weather_df, sales_df, on='date', how='inner')
    joined_df.to_csv(os.path.join(data_dir, 'joined_data.csv'), index=False)
    print(f"Joined dataset saved with {len(joined_df)} records.")
 
def train_ml_model():
    data_dir = '/opt/airflow/data'
    
    # Обучаемся на исходных данных (без фильтрации)
    original_weather = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    original_sales = pd.read_csv(os.path.join(data_dir, 'sales_data.csv'))
    
    # Объединяем исходные данные
    original_df = pd.merge(original_weather, original_sales, on='date', how='inner')
    
    # Проверяем, есть ли данные
    if len(original_df) == 0:
        print("Warning: No data for training. Model training skipped.")
        return
    
    X = original_df[['temperature']]
    y = original_df['sales']
    
    model = LinearRegression()
    model.fit(X, y)
    
    joblib.dump(model, os.path.join(data_dir, 'ml_model.pkl'))
    print(f"ML model trained on {len(original_df)} samples and saved.")
 
def deploy_ml_model():
    data_dir = '/opt/airflow/data'
    model_path = os.path.join(data_dir, 'ml_model.pkl')
    
    if os.path.exists(model_path):
        model = joblib.load(model_path)
        print("Model deployed successfully:", model)
    else:
        print("No model found to deploy.")
 
# Инициализация операторов
t1 = PythonOperator(task_id="fetch_weather_forecast", python_callable=fetch_weather_forecast, dag=dag)
t2 = PythonOperator(task_id="clean_weather_data", python_callable=clean_weather_data, dag=dag)
t3 = PythonOperator(task_id="fetch_sales_data", python_callable=fetch_sales_data, dag=dag)
t4 = PythonOperator(task_id="clean_sales_data", python_callable=clean_sales_data, dag=dag)
t5 = PythonOperator(task_id="join_datasets", python_callable=join_datasets, dag=dag)
t6 = PythonOperator(task_id="train_ml_model", python_callable=train_ml_model, dag=dag)
t7 = PythonOperator(task_id="deploy_ml_model", python_callable=deploy_ml_model, dag=dag)
 
# Настройка зависимостей (граф)
t1 >> t2
t3 >> t4
[t2, t4] >> t5
t5 >> t6 >> t7