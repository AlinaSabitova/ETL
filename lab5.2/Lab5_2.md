# Лабораторная работа работа 5.2 Разработка алгоритмов для трансформации данных. Airflow DAG

# Цель работы

1. Закрепить навыки развертывания Apache Airflow в контейнеризированной среде (Docker).
2. Изучить работу с JSON-данными и бинарным контентом (изображениями) внутри ETL-процесса.
3. Научиться проектировать архитектуру ETL-решений и визуализировать её.
4. Автоматизировать выгрузку результатов работы DAG из контейнера в хост-систему.

## Индивидуальное задание

| Вариант | Задание 1 (Анализ/ETL) | Задание 2 (Обработка/Логика) | Задание 3 (Отчетность/Метрики) |
|---------|------------------------|------------------------------|-------------------------------|
| 12 | Отчет по незагруженным изображениям | Мониторинг успешных запусков (Success/Fail callback) | Анализ уязвимостей реализации DAG |

# Архитектура решения

```mermaid
---
config:
  layout: elk
---
flowchart LR
    API((🚀 Launch Library 2 API))

    subgraph Docker["🐳 Docker"]
        direction TB
        Airflow["⚙️ Airflow<br/>DAG: listing_sabitova_rocket"]
        Jupyter["🧠 Jupyter<br/>CLIP ML"]
        Streamlit["📊 Streamlit<br/>Dashboard"]
    end

    subgraph Host["💻 Ubuntu"]
        direction TB
        Data[(📁 ./data)]
        Dags[(📁 ./dags<br/>DAG файл)]
        Logs[(📁 ./logs)]
    end

    API -->|1. Получение данных о запусках| Airflow
    Airflow -->|2. Сохранение JSON и фото| Data
    Airflow -.->|3. Запись логов| Logs
    Dags -.->|4. Чтение DAG файла| Airflow
    Data -->|5. Передача изображений| Jupyter
    Jupyter -->|6. Сохранение предсказаний| Data
    Data -->|7. Чтение всех отчетов| Streamlit

    style API fill:#ce93d8,stroke:#6a1b9a
    style Airflow fill:#ffcc80,stroke:#e65100
    style Jupyter fill:#a5d6a7,stroke:#2e7d32
    style Streamlit fill:#81d4fa,stroke:#01579b
    style Data fill:#ffe0b2,stroke:#f57c00
    style Dags fill:#ffcdd2,stroke:#c62828
    style Logs fill:#ffcdd2,stroke:#c62828
```

# Пояснение к архитектуре

Архитектура построена по принципу микросервисов с общим разделяемым хранилищем (Shared Volumes). Это позволяет сервисам обмениваться файлами без сложной сетевой пересылки.

#### Внешние источники и Пользователь
- **Launch Library 2 API** — внешний REST API, откуда Airflow получает данные о запусках и ссылки на фото.
- **Пользователь** — взаимодействует с системой через браузер (порты 8080, 8888, 8501).

#### Хост-система и локальные папки (Bind Mounts)
На Ubuntu проброшены папки:
- `./dags` — код DAG (`listing_sabitova_rocket.py`)
- `./data` — озеро данных (JSON, фото, отчеты)
- `./logs` — логи Airflow

#### Apache Airflow (ETL контур)
- **Scheduler** — запускает DAG, загружает данные из API, скачивает фото.
- **Webserver** — UI для управления и мониторинга.
- **PostgreSQL** — хранит метаданные Airflow.

#### Jupyter (ML контур)
- Запускает CLIP нейросеть для классификации фото ракет.
- Сохраняет предсказания в `ml_predictions.csv`.

#### Streamlit (Аналитический контур)
- Визуализирует отчеты.
- Отображает галерею фото с тегами и графики.

# Технический стек

- **Оркестрация**: Apache Airflow 2.8.1
- **Контейнеризация**: Docker, Docker Compose
- **Язык программирования**: Python 3.11-slim
- **Библиотеки (ETL & ML)**: Pandas, Scikit-learn, Joblib, Requests, Torch, Transformers, Pillow
- **Визуализация**: Streamlit, Plotly, Matplotlib
- **База данных**: PostgreSQL 12 (для метаданных Airflow)
- **Источник данных**: Launch Library 2 API

# Логика работы DAG

```mermaid
graph TD
    START([Start]) --> CLEAN

    CLEAN[clean_data_directory<br/>Очистка папки data]

    CLEAN --> DOWNLOAD_JSON[download_launches<br/>Загрузка JSON из API]

    DOWNLOAD_JSON --> DOWNLOAD_IMAGES[download_pictures<br/>Скачивание фото]

    DOWNLOAD_IMAGES --> REPORT_FAILED[report_failed_images<br/>Отчет о неудачных фото]

    REPORT_FAILED --> ANALYZE[analyze_vulnerabilities<br/>Поиск уязвимостей в коде]

    ANALYZE --> NOTIFY[notify<br/>Уведомление]

    NOTIFY --> END([End])

    DOWNLOAD_JSON --> SUCCESS[on_success_callback<br/>Мониторинг успешных запусков]
    DOWNLOAD_JSON --> FAILURE[on_failure_callback<br/>Логирование ошибок DAG]

    style REPORT_FAILED fill:#ffe6cc,stroke:#d79b00
    style ANALYZE fill:#f8cecc,stroke:#b85450
    style SUCCESS fill:#e1d5e7,stroke:#9673a6
    style FAILURE fill:#e1d5e7,stroke:#9673a6
```

# Описание DAG

| Функция | Описание |
|---------|----------|
| `clean_data_directory` | Очистка папки `/data` перед новым запуском |
| `download_launches` | Получение данных о запусках из Launch Library 2 API, сохранение в `launches.json` |
| `download_pictures` | Парсинг JSON, скачивание фотографий ракет из API, сохранение в `images/` |
| `report_failed_images` | Формирование отчета о неудачных загрузках изображений, сохранение в `failed_images_report.json` |
| `analyze_vulnerabilities` | Анализ кода DAG на наличие уязвимостей (хардкод секретов, отсутствие timeout, bare except), сохранение в `vulnerability_analysis_*.json` |
| `notify` | Вывод сообщения об успешном завершении DAG |
| `on_success_callback` | Колбэк при успешном выполнении DAG: анализ статусов запусков, сохранение в `launch_monitoring_*.json` |
| `on_failure_callback` | Колбэк при ошибке DAG: логирование ошибки, сохранение в `dag_failure_*.json` |

# Структура проекта

<img width="178" height="282" alt="image" src="https://github.com/user-attachments/assets/92b499c5-9584-4307-aa41-2364ab4ff737" />

## Dockerfile

Создает кастомный образ на основе Apache Airflow, устанавливает библиотеки (pandas, torch, transformers, streamlit и др.) и создает директории для данных, логов и приложения.

```
FROM apache/airflow:slim-2.8.1-python3.11
 
USER root
 
# Создаём директории и назначаем владельца (ID 50000 - стандартный пользователь airflow)
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app \
    && chown -R 50000:0 /opt/airflow/data /opt/airflow/logs /opt/airflow/app
 
USER airflow
 
# Настройка pip с зеркалами для ускорения загрузки
RUN mkdir -p /home/airflow/.pip && \
    echo "[global]" > /home/airflow/.pip/pip.conf && \
    echo "index-url = https://mirrors.aliyun.com/pypi/simple/" >> /home/airflow/.pip/pip.conf && \
    echo "trusted-host = mirrors.aliyun.com" >> /home/airflow/.pip/pip.conf && \
    echo "timeout = 120" >> /home/airflow/.pip/pip.conf
 
# Устанавливаем необходимые Python-библиотеки (разбиваем на группы для лучшей отказоустойчивости)
RUN pip install --no-cache-dir --default-timeout=1000 \
    pandas \
    scikit-learn \
    joblib \
    requests \
    pillow \
    plotly \
    psycopg2-binary
 
# Устанавливаем streamlit (отдельно, т.к. у него много зависимостей)
RUN pip install --no-cache-dir --default-timeout=1000 streamlit
 
# Устанавливаем jupyter
RUN pip install --no-cache-dir --default-timeout=1000 jupyter
 
# Устанавливаем PyTorch и transformers (самые тяжелые, могут быть проблемы)
# Используем CPU версию для совместимости
RUN pip install --no-cache-dir --default-timeout=1000 \
    torch \
    torchvision \
    transformers
```

## docker-compose.yml

Оркестрирует 6 сервисов:
- **postgres** — база данных для метаданных Airflow
- **init** — инициализация БД и создание пользователя admin
- **webserver** — веб-интерфейс Airflow (порт 8080)
- **scheduler** — планировщик задач Airflow
- **streamlit** — дашборд визуализации (порт 8501)
- **jupyter** — среда для ML обработки (порт 8888)

Все сервисы используют общие папки `./dags`, `./data`, `./logs`

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
  - AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
  - AIRFLOW__CORE__DEFAULT_TIMEZONE=utc

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

  init:
    image: *airflow_image
    depends_on:
      postgres:
        condition: service_healthy
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - ./logs:/opt/airflow/logs
    entrypoint: >
      bash -c "
      airflow db upgrade &&
      airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.org &&
      echo 'Airflow init completed.'"

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
      - ./logs:/opt/airflow/logs
    command: webserver

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
      - ./logs:/opt/airflow/logs
    command: scheduler

  streamlit:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8501:8501"
    volumes:
      - ./data:/opt/airflow/data
      - ./app:/opt/airflow/app
    command: bash -c "streamlit run /opt/airflow/app/app.py --server.port=8501 --server.address=0.0.0.0"
  
  jupyter:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8888:8888"
    volumes:
      - ./:/opt/airflow/project
      - ./data:/opt/airflow/project/data
    working_dir: /opt/airflow/project
    command: bash -c "jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''"

volumes:
  postgres_data:
```

## dags/listing_sabitova_rocket.py

DAG реализует ETL-пайплайн для сбора данных о космических запусках, скачивания фотографий ракет и выполнения трех индивидуальных заданий.

```
import json
import pathlib
import os
import sys
from datetime import datetime
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
 
# --- Конфигурационные переменные ---
DATA_DIR = "/opt/airflow/data"
IMAGES_DIR = f"{DATA_DIR}/images"
TMP_JSON_FILE = "/tmp/launches.json"
MAX_IMAGES = 10
API_URL = f"https://ll.thespacedevs.com/2.3.0/launches/upcoming/?format=json&mode=list&limit={MAX_IMAGES}"
 
DATABASE_PASSWORD = "PostgresPass2024!"  # ⚠️ УЧЕБНЫЙ ПРИМЕР
 
# --- Задание 1 ---
def report_failed_images(**context):
    failed_images = context['ti'].xcom_pull(task_ids='download_pictures', key='failed_images')
    if not failed_images:
        failed_images = []
 
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_failed": len(failed_images),
        "failed_images": failed_images,
        "task_id": "download_pictures"
    }
 
    pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
 
    report_file = f"{DATA_DIR}/failed_images_report.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
 
    return report
 
API_TOKEN = "sk_live_XXXX"  # ⚠️ УЧЕБНЫЙ ПРИМЕР
 
# --- Задание 2 ---
def check_launch_success_callback(context):
    dag_run = context['dag_run']
    execution_date = context.get('execution_date') or dag_run.execution_date
 
    monitoring_report = {
        "execution_date": execution_date.isoformat() if execution_date else None,
        "status": "SUCCESS",
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "successful_launches_count": 1,
        "failed_launches_count": 0,
        "duration_seconds": None
    }
 
    if dag_run.end_date and dag_run.start_date:
        monitoring_report["duration_seconds"] = round(
            (dag_run.end_date - dag_run.start_date).total_seconds(), 2
        )
 
    pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
 
    report_file = f"{DATA_DIR}/launch_monitoring_{execution_date.strftime('%Y%m%d_%H%M%S')}.json"
 
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(monitoring_report, f, indent=2, ensure_ascii=False)
 
def check_launch_failure_callback(context):
    dag_run = context['dag_run']
    execution_date = context.get('execution_date') or dag_run.execution_date
    exception = context.get('exception')
 
    report = {
        "execution_date": execution_date.isoformat() if execution_date else None,
        "status": "FAILED",
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "failed_launches_count": 1,
        "successful_launches_count": 0,
        "error_message": str(exception) if exception else "Unknown error"
    }
 
    pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
 
    report_file = f"{DATA_DIR}/launch_monitoring_{execution_date.strftime('%Y%m%d_%H%M%S')}.json"
 
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
 
JWT_SECRET = "secret"  # ⚠️ УЧЕБНЫЙ ПРИМЕР
 
# --- Загрузка картинок ---
def _get_pictures_with_tracking(**context):
    pathlib.Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)
 
    failed_images = []
    successful_images = []
 
    with open(TMP_JSON_FILE, encoding="utf-8") as f:
        launches = json.load(f)
 
    image_urls = []
    for launch in launches.get("results", []):
        image = launch.get("image")
        if isinstance(image, dict):
            url = image.get("image_url")
        else:
            url = image
        if url:
            image_urls.append(url)
 
    image_urls = list(dict.fromkeys(image_urls))[:MAX_IMAGES]
 
    for i, url in enumerate(image_urls, 1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
 
            filename = url.split("/")[-1].split("?")[0] or f"image_{i}.jpg"
            path = f"{IMAGES_DIR}/{filename}"
 
            with open(path, "wb") as f:
                f.write(response.content)
 
            successful_images.append({"url": url})
        except Exception as e:
            failed_images.append({"url": url, "error": str(e)})
 
    context['ti'].xcom_push(key='failed_images', value=failed_images)
 
    return {
        "successful": len(successful_images),
        "failed": len(failed_images)
    }
 
# --- 🔥 ИСПРАВЛЕННАЯ ОЧИСТКА ---
clean_data_directory = BashOperator(
    task_id="clean_data_directory",
    bash_command=f"""
    mkdir -p {DATA_DIR} &&
    mkdir -p {IMAGES_DIR} &&
 
    # очищаем только картинки
    rm -rf {IMAGES_DIR}/* &&
 
    # удаляем только временный файл
    rm -f {TMP_JSON_FILE} &&
 
    # удаляем старый launches.json (чтобы обновлялся)
    rm -f {DATA_DIR}/launches.json
    """,
)
 
# --- DAG ---
dag = DAG(
    dag_id="listing_sabitova_rocket",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    catchup=False,
    on_success_callback=check_launch_success_callback,
    on_failure_callback=check_launch_failure_callback
)
 
download_launches = BashOperator(
    task_id="download_launches",
    bash_command=f"curl -o {TMP_JSON_FILE} '{API_URL}' && cp {TMP_JSON_FILE} {DATA_DIR}/launches.json",
    dag=dag,
)
 
download_pictures = PythonOperator(
    task_id="download_pictures",
    python_callable=_get_pictures_with_tracking,
    dag=dag
)
 
report_failed = PythonOperator(
    task_id="report_failed_images",
    python_callable=report_failed_images,
    dag=dag
)
 
notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "DAG finished"',
    dag=dag,
)
 
clean_data_directory >> download_launches >> download_pictures >> report_failed >> notify
```

## app/app.py

Визуализирует результаты работы DAG через 4 вкладки:
- **Основная аналитика** — таблица запусков, графики по провайдерам и статусам, галерея фото с ML-тегами
- **Отчет по изображениям** — статистика и детали неудачных загрузок фото (Задание 1)
- **Мониторинг запусков** — количество успешных и общих запусков (Задание 2)
- **Анализ уязвимостей** — количество и типы найденных уязвимостей (Задание 3)


```
import streamlit as st
import pandas as pd
import json
import os
from PIL import Image
import glob

st.set_page_config(page_title="Аналитика космических запусков", layout="wide")

DATA_DIR = "/opt/airflow/data"
JSON_FILE = f"{DATA_DIR}/launches.json"
PREDICTIONS_FILE = f"{DATA_DIR}/ml_predictions.csv"
IMAGES_DIR = f"{DATA_DIR}/images"

st.title("🚀 Аналитика космических запусков")
st.markdown("### Вариант 12: Мониторинг успешных запусков и анализ уязвимостей")

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Основная аналитика",
    "📸 Отчет по изображениям",
    "📈 Мониторинг запусков",
    "🔒 Анализ уязвимостей"
])

# --- Вкладка 1: Основная аналитика ---
with tab1:
    st.header("Ближайшие запуски")
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r") as f:
            launches = json.load(f).get("results", [])
        if launches:
            df_launches = pd.DataFrame([{
                "Имя миссии": l.get("name"),
                "Статус": l.get("status", {}).get("name"),
                "Провайдер": l.get("launch_service_provider", {}).get("name")
            } for l in launches])
            st.dataframe(df_launches)
            
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Запуски по провайдерам")
                provider_counts = df_launches["Провайдер"].value_counts()
                if not provider_counts.empty:
                    st.bar_chart(provider_counts)
            with col2:
                st.subheader("Статусы запусков")
                status_counts = df_launches["Статус"].value_counts()
                if not status_counts.empty:
                    st.bar_chart(status_counts)
    else:
        st.warning("Файл launches.json еще не загружен")
    
    st.markdown("---")
    
    st.header("🧠 Распознавание типов ракет (ML)")
    if os.path.exists(PREDICTIONS_FILE):
        df_preds = pd.read_csv(PREDICTIONS_FILE)
        st.dataframe(df_preds)
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Статистика по типам ракет")
            rocket_counts = df_preds["predicted_rocket"].value_counts().sort_values(ascending=False)
            if not rocket_counts.empty:
                st.bar_chart(rocket_counts)
        
        with col2:
            st.subheader("Средняя уверенность по типам")
            avg_confidence = df_preds.groupby("predicted_rocket")["confidence"].mean().sort_values(ascending=False)
            if not avg_confidence.empty:
                st.bar_chart(avg_confidence)
        
        st.subheader("🖼️ Галерея распознанных ракет")
        cols = st.columns(3)
        for idx, row in df_preds.head(9).iterrows():
            img_path = os.path.join(IMAGES_DIR, row['image_name'])
            if os.path.exists(img_path):
                with cols[idx % 3]:
                    img = Image.open(img_path)
                    st.image(img, caption=f"{row['predicted_rocket']} ({row['confidence']}%)", use_container_width=True)
    else:
        st.info("Результаты ML еще не готовы. Запустите ml.ipynb")

# --- Вкладка 2: Отчет по незагруженным изображениям ---
with tab2:
    st.header("📸 Отчет по незагруженным изображениям")
    report_files = glob.glob(f"{DATA_DIR}/failed_images_report.json")
    if report_files:
        with open(report_files[0], 'r') as f:
            report = json.load(f)
        st.metric("Всего неудачных загрузок", report.get('total_failed', 0))
        if report.get('failed_images'):
            df_failed = pd.DataFrame(report['failed_images'])
            st.dataframe(df_failed)
            if 'error' in df_failed.columns:
                st.bar_chart(df_failed['error'].value_counts())
    else:
        st.info("Отчет пока не создан")

# --- Вкладка 3: Мониторинг запусков ---
with tab3:
    st.header("📈 Мониторинг запусков")
    monitoring_files = glob.glob(f"{DATA_DIR}/launch_monitoring_*.json")
    if monitoring_files:
        with open(monitoring_files[-1], 'r') as f:
            report = json.load(f)
        col1, col2 = st.columns(2)
        with col1:
            st.metric("✅ Успешные запуски", report.get('successful_launches', 0))
        with col2:
            st.metric("📊 Всего запусков", report.get('total_launches', 0))
    else:
        st.info("Отчеты мониторинга пока не созданы")

# --- Вкладка 4: Анализ уязвимостей ---
with tab4:
    st.header("🔒 Анализ уязвимостей")
    vuln_files = glob.glob(f"{DATA_DIR}/vulnerability_analysis_*.json")
    if vuln_files:
        with open(vuln_files[-1], 'r') as f:
            vuln_report = json.load(f)
        st.metric("Всего уязвимостей", vuln_report.get('total_vulnerabilities', 0))
        if vuln_report.get('vulnerabilities'):
            for vuln in vuln_report['vulnerabilities']:
                st.write(f"- **{vuln['type']}** [{vuln['severity']}]")
    else:
        st.info("Анализ уязвимостей пока не выполнен")
```

## ml.ipynb

Jupyter ноутбук для ML обработки:
- Загружает фотографии из `./data/images/`
- Прогоняет их через нейросеть CLIP (Zero-Shot Classification)
- Сохраняет предсказания в `ml_predictions.csv`

# Ход выполнения

Установка правильных прав доступа для Airflow (UID 50000):

<img width="957" height="46" alt="Снимок экрана 2026-03-29 215346" src="https://github.com/user-attachments/assets/c8dadf5f-b280-4a6e-a0cf-c0b3f92df5fe" />

Сборка кастомного образа с ML и Streamlit:

<img width="956" height="307" alt="Снимок экрана 2026-03-29 215353" src="https://github.com/user-attachments/assets/3d2bee55-2f66-41cd-a227-167cc07327c3" />

Запуск инфраструктуры в фоновом режиме:

<img width="945" height="57" alt="Снимок экрана 2026-03-29 215409" src="https://github.com/user-attachments/assets/1c595f1c-9e6f-4cd9-80ad-ff3a81cd9085" />

<img width="950" height="191" alt="Снимок экрана 2026-03-29 215451" src="https://github.com/user-attachments/assets/f347a258-4e5c-4b66-a154-30bc22e2f3cd" />

Переходим в браузер по адресу http://localhost:8080. Откроется Airflow:

<img width="1211" height="738" alt="Снимок экрана 2026-03-29 215547" src="https://github.com/user-attachments/assets/9b1083be-9e35-4c3e-9a34-eb32cdd3e4ac" />

Видим наш даг, запустим его:

<img width="1209" height="617" alt="Снимок экрана 2026-03-29 215558" src="https://github.com/user-attachments/assets/e7230d80-ee48-48da-a02c-889ec67b22c7" />

<img width="1217" height="730" alt="Снимок экрана 2026-03-31 022127" src="https://github.com/user-attachments/assets/14c6df7e-bbe0-4aeb-999a-6b986509e6ce" />

Схема дага в Airflow:

<img width="883" height="619" alt="Снимок экрана 2026-03-31 022052" src="https://github.com/user-attachments/assets/97d9a002-9b0c-40ed-a0d3-0a6398aaa704" />

Диаграмма Ганта:

<img width="846" height="291" alt="Снимок экрана 2026-03-31 022102" src="https://github.com/user-attachments/assets/b3da9f84-ab70-4cf6-a19f-35e63eaa26c3" />

Теперь перейдем в Jupyter по адресу http://localhost:8888 и запустим все коды ML-модели:

<img width="1226" height="741" alt="Снимок экрана 2026-03-31 023518" src="https://github.com/user-attachments/assets/21ccbf2a-174b-471d-aa12-f26822d8ac6a" />

<img width="1217" height="735" alt="Снимок экрана 2026-03-31 023532" src="https://github.com/user-attachments/assets/6c983add-cf17-4143-b406-dc4fc826698d" />

<img width="1218" height="744" alt="Снимок экрана 2026-03-31 023544" src="https://github.com/user-attachments/assets/c558400f-73a2-4fa0-8877-f6b312b43b32" />

Видим, что все коды выполнились успешно

Теперь посмотрим аналитику в Streamlit по адресу http://localhost:8501 :

<img width="1218" height="734" alt="image" src="https://github.com/user-attachments/assets/7dc7aa5d-d746-483b-9801-9e834691a917" />

Есть 4 вкладки. Начнем с первой - основного задания. Здесь отображается информация о ближайших запусках, статусах запусков и результатов выполнения модели машинного обучения:

<img width="1153" height="596" alt="image" src="https://github.com/user-attachments/assets/1b8a02c0-9ec2-458f-8efb-0848c6468d9b" />

<img width="623" height="521" alt="image" src="https://github.com/user-attachments/assets/f5b46982-63c5-4b4a-9fdf-ca2442c49c55" />

<img width="1180" height="379" alt="image" src="https://github.com/user-attachments/assets/d42cb7b3-617f-4bc2-a560-b05b9e62f603" />

<img width="1131" height="473" alt="image" src="https://github.com/user-attachments/assets/c4cf14ef-be4c-41a5-b5b4-c907c3efd88d" />

<img width="1116" height="632" alt="image" src="https://github.com/user-attachments/assets/6b146efc-8719-45c4-803e-a0c5fd0b739f" />

## Первое индивидуальное

На второй вкладке выводится отчет по незагруженным изображениям. В данном случае их нет, все изображения загрузились успешно:

<img width="1159" height="530" alt="image" src="https://github.com/user-attachments/assets/77596271-37a1-4b24-8a13-3e461fe54fc7" />

## Второе индивидуальное

На следующей вкладке выводится статистика по запускам дага: ключевые показатели, график,таблица с историей запусков:

<img width="1117" height="384" alt="image" src="https://github.com/user-attachments/assets/0956d40b-f1cd-40a6-b60e-2b8f02a7f275" />

<img width="1153" height="492" alt="image" src="https://github.com/user-attachments/assets/50480fd5-9265-49bc-b505-cd1bcea148b4" />

<img width="1131" height="527" alt="image" src="https://github.com/user-attachments/assets/e9218ae8-62ec-48ab-9476-c43c7dcf820b" />

## Третье индивидуальное

На последней вкладке выводится анализ дага на наличие уязвимостей:

В даг целенаправленно было добавленно несколько видов уязвимостей, чтобы можно было проверить корректность работы функции.

В результате выводится общее число уязвимостей, график распределения уязвимостей по критичности и все обнаруженные уязвимости с указанием строки и выводом соответствующего фрагмента кода:

<img width="1095" height="363" alt="image" src="https://github.com/user-attachments/assets/5a2b1bf8-c636-4613-bacf-e2116434799d" />

<img width="1146" height="466" alt="image" src="https://github.com/user-attachments/assets/65afb392-5672-4948-970e-95787984751b" />

<img width="1137" height="607" alt="image" src="https://github.com/user-attachments/assets/f48eecca-9bdd-4315-bd01-30ec2b0de901" />

# Выводы

В ходе работы выполнены все поставленные задачи:

1. Развернут Apache Airflow в Docker — настроена оркестрация шести контейнеров с общим хранилищем через bind mounts.
2. Реализован ETL-пайплайн — загрузка JSON и изображений из Launch Library API с сохранением в общую папку.
3. Спроектирована архитектура — выделены слои: источник данных, хранение, бизнес-логика; выполнена визуализация в Mermaid.
4. Автоматизирован экспорт — скрипт export_data.sh выгружает все результаты из контейнеров на хост с архивацией.

Индивидуальные задания выполнены.
