# Лабораторная работа 5.1. Проектирование объектной модели данных. Проектирование сквозного конвейера ETL

# Цель работы:

1. Развернуть среду оркестрации Apache Airflow с использованием Docker.
2. Изучить структуру и принципы работы ETL-конвейеров (DAG).
3. Спроектировать архитектуру аналитического решения.
4. Реализовать ETL-процесс получения погодных данных через API и их обработки.
5. Использовать обученную ML-модель для прогнозирования бизнес-метрик (продаж).

# Индивидуальное задание. Вариант 12

| Вариант | Задание 1 (Сбор данных) | Задание 2 (Трансформация) | Задание 3 (Сохранение/Визуализация) |
|---------|------------------------|--------------------------|-------------------------------------|
| 12 | Прогноз: Амстердам, 7 дней | Фильтр: t < 0°C | Вывести среднюю температуру |

Необходимо:
1. Получить прогноз погоды в Амстердаме на 7 дней (используя внешний API).
2. Обработать данные: отфильтровать дни, в которые температура опускалась ниже 0.
3. Сгенерировать данные о продажах за эти же даты и объединить наборы данных.
4. Обучить простейшую ML-модель (Линейная регрессия).
6. Вывести среднюю температуру за 7 дней (в Streamlit).

# Архитектура проекта

<img width="911" height="607" alt="image" src="https://github.com/user-attachments/assets/42a696f1-7fc6-4826-9795-da98d51a4f60" />

# Технический стек

- **Оркестрация**: Apache Airflow 2.8.1
- **Контейнеризация**: Docker, Docker Compose
- **Язык программирования**: Python 3.11
- **Библиотеки (ETL & ML)**: Pandas, Scikit-learn, Joblib, Requests
- **Визуализация**: Streamlit, Matplotlib
- **База данных**: PostgreSQL 12 (для метаданных Airflow)

# Описание DAG

| Функция | Описание |
|---------|----------|
| `fetch_weather_forecast` | Получение прогноза погоды в Амстердаме на 7 дней (Open-Meteo API), сохранение в `weather_forecast.csv` |
| `clean_weather_data` | Очистка данных, фильтрация (t < 0°C), сохранение в `clean_weather.csv` |
| `fetch_sales_data` | Генерация моковых данных о продажах, сохранение в `sales_data.csv` |
| `clean_sales_data` | Очистка данных о продажах, сохранение в `clean_sales.csv` |
| `join_datasets` | Объединение погодных данных и продаж по дате, сохранение в `joined_data.csv` |
| `train_ml_model` | Обучение модели линейной регрессии (температура → продажи), сохранение в `ml_model.pkl` |
| `deploy_ml_model` | Загрузка и проверка модели |

# Структура

 Структура директорий:

 <img width="677" height="383" alt="image" src="https://github.com/user-attachments/assets/34cd4d11-afd3-4411-973f-e7eec2c5fbd9" />

## Dockerfile

Создает кастомный образ на основе Apache Airflow, устанавливает необходимые библиотеки, а также создает директории для данных, логов и приложения с правами пользователя airflow:

```
FROM apache/airflow:slim-2.8.1-python3.11

USER airflow

# Устанавливаем необходимые Python-библиотеки
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    joblib \
    requests \
    azure-storage-blob==12.8.1 \
    psycopg2-binary \
    streamlit \
    matplotlib \
    "connexion[swagger-ui]"

USER root

# Создаём директории и назначаем владельца
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app \
    && chown -R airflow: /opt/airflow/data /opt/airflow/logs /opt/airflow/app

USER airflow
```

## docker-compose.yml

Описывает инфраструктуру из пяти сервисов: PostgreSQL для хранения метаданных Airflow, init для инициализации базы данных и создания пользователя, веб-сервер Airflow на порту 8080, планировщик задач и Streamlit для визуализации на порту 8501, все сервисы объединены в общую сеть:

```
x-environment: &airflow_environment
  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
  - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
  - AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
  - AIRFLOW__CORE__LOAD_EXAMPLES=False
  - AIRFLOW__CORE__STORE_DAG_CODE=True
  - AIRFLOW__CORE__STORE_SERIALIZED_DAGS=True
  - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
  - AIRFLOW__WEBSERVER__RBAC=False
  - AIRFLOW__WEBSERVER__SECRET_KEY=supersecretkey123
  - AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
  - AIRFLOW__LOGGING__REMOTE_LOGGING=False
  - AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs

x-airflow-image: &airflow_image custom-airflow:slim-2.8.1-python3.11

services:
  postgres:
    image: postgres:12-alpine
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - airflow-network

  init:
    image: *airflow_image
    depends_on:
      postgres:
        condition: service_healthy
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - logs:/opt/airflow/logs
    entrypoint: >
      bash -c "
      sleep 5 &&
      airflow db upgrade &&
      airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.org &&
      echo 'Airflow init completed.'"
    healthcheck:
      test: ["CMD", "airflow", "db", "check"]
      interval: 10s
      retries: 5
      start_period: 10s
    networks:
      - airflow-network 

  webserver:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8080:8080"
    restart: always
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - logs:/opt/airflow/logs
    command: webserver
    networks:
      - airflow-network  

  scheduler:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    restart: always
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - logs:/opt/airflow/logs
    command: scheduler
    networks:
      - airflow-network  

  streamlit:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8501:8501"
    restart: always
    volumes:
      - ./data:/opt/airflow/data
      - ./app:/opt/airflow/app
    command: bash -c "streamlit run /opt/airflow/app/app.py --server.port=8501 --server.address=0.0.0.0"
    networks:
      - airflow-network

networks:  
  airflow-network:
    driver: bridge

volumes:
  logs:
  postgres_data:
```

## dags/real_umbrella.py

Реализует ETL-пайплайн: получает прогноз погоды в Амстердаме на 7 дней через Open-Meteo API, очищает данные, фильтрует дни с температурой ниже 0°C, генерирует моковые данные о продажах, объединяет их, обучает модель линейной регрессии для прогнозирования продаж на основе температуры и сохраняет модель для дальнейшего использования:

```
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
```

## app/app.py

Создает Streamlit-дашборд, который визуализирует данные о погоде в Амстердаме: загружает исходный прогноз на 7 дней из Open-Meteo API, отображает его в виде таблицы, затем показывает отфильтрованные дни с температурой ниже 0°C (либо сообщение об их отсутствии) и выводит среднюю температуру:

```
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
```

# Запуск

Для того, чтобы у Airflow были права создать файл в папке data, меняем владельца этой папки и всех файлов внутри на пользователя с UID 50000 и группу root. Затем меняем владельца всей папки проекта и всех файлов внутри на пользователя dev:

<img width="924" height="63" alt="image" src="https://github.com/user-attachments/assets/bfbffb7e-3630-44c5-9b18-bf6e4beeac8e" />

Собираем Docker-образ:

<img width="927" height="246" alt="image" src="https://github.com/user-attachments/assets/debd8496-329a-49b8-ab76-f134585b7202" />

Запускаем контейнеры:

<img width="927" height="174" alt="image" src="https://github.com/user-attachments/assets/61647ab8-b0de-4de3-b235-579a86f50309" />

Проверяем, что все контейнеры запущены:

<img width="932" height="194" alt="image" src="https://github.com/user-attachments/assets/663acef6-8187-4ca9-819d-707f6bb9a926" />

Переходим в браузер по адресу: http://localhost:8080. Откроется Airflow, авторизуемся:

<img width="1211" height="610" alt="Снимок экрана 2026-03-21 002729" src="https://github.com/user-attachments/assets/e5f7ac3c-49db-468a-b849-58733155703b" />

Видим, что у нас есть DAG:

<img width="1221" height="745" alt="Снимок экрана 2026-03-21 002854" src="https://github.com/user-attachments/assets/bc2956b1-3582-423e-8168-5549bb093c84" />

Запустим его:

<img width="1213" height="587" alt="Снимок экрана 2026-03-21 015136" src="https://github.com/user-attachments/assets/ca155190-b93e-4f4b-a944-2af61b5338d5" />

Видим, что DAG успешно выполнился.

Посмотрим схему графа и диаграмму Ганта:

<img width="1218" height="612" alt="Снимок экрана 2026-03-21 015210" src="https://github.com/user-attachments/assets/761bf9da-19cf-436a-be00-133968525fca" />

<img width="1214" height="572" alt="Снимок экрана 2026-03-21 015157" src="https://github.com/user-attachments/assets/c1e7c3a7-64f4-467c-be92-face360047e5" />

Теперь перейдем в Streamlit (http://localhost:8501):

<img width="1194" height="776" alt="Снимок экрана 2026-03-21 015231" src="https://github.com/user-attachments/assets/0db0094e-253d-492d-992c-5b85636b402a" />

Уже здесь видим, что за семь дней не было ни одного, когда температура опускалась бы ниже 0.

Получаем уведомление об этом:

<img width="1168" height="179" alt="Снимок экрана 2026-03-21 015238" src="https://github.com/user-attachments/assets/6c8f0ed9-231d-46a0-97ab-8f8ce2a915f9" />

Теперь посмотрим на среднюю температуру за 7 дней:

<img width="1186" height="186" alt="Снимок экрана 2026-03-21 015244" src="https://github.com/user-attachments/assets/5e603d93-75c0-41a4-9066-a89116a29b79" />

Перейдем в Colab и запустим модель машинного обучения. Посмотрим прогноз, сколько зонтиков будет продано в день с указанной температурой (подставляем среднюю температуру из Streamlit):

<img width="1209" height="612" alt="image" src="https://github.com/user-attachments/assets/c12fa8e1-2475-48e5-b324-ba827ce5a9fb" />

Видим, что по прогнозу при почти 8 градусах будет продано около 15 зонтиков.

# Выводы

В ходе выполнения лабораторной работы были выполнены следующие задачи:

1. **Развернута среда Apache Airflow 2.8.1** с использованием Docker. Настроены все необходимые сервисы: PostgreSQL, веб-сервер, планировщик, Streamlit.

2. **Изучена структура DAG** в Airflow. Реализован ETL-конвейер из 7 задач с зависимостями: сбор → очистка → объединение → обучение → деплой.

3. **Спроектирована архитектура** решения: сбор данных (API), трансформация (очистка, фильтрация t < 0°C), ML-модель (линейная регрессия), визуализация (Streamlit), оркестрация (Airflow).

4. **Реализован ETL-процесс**: получение прогноза погоды для Амстердама через Open-Meteo API, очистка, фильтрация, объединение с данными о продажах.

5. **Обучена ML-модель** для прогнозирования продаж на основе температуры.
