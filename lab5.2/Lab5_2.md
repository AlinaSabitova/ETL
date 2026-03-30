# Лабораторная работа работа 5.2 Разработка алгоритмов для трансформации данных. Airflow DAG

# Цель работы

1. Закрепить навыки развертывания Apache Airflow в контейнеризированной среде (Docker).
2. Изучить работу с JSON-данными и бинарным контентом (изображениями) внутри ETL-процесса.
3. Научиться проектировать архитектуру ETL-решений и визуализировать её.
4. Автоматизировать выгрузку результатов работы DAG из контейнера в хост-систему.

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
- Визуализирует отчеты:
  - **Задание 1** — `failed_images_report.json`
  - **Задание 2** — `launch_monitoring_*.json`, `dag_failure_*.json`
  - **Задание 3** — `vulnerability_analysis_*.json`
- Отображает галерею фото с тегами и графики.

# Технический стек

- **Оркестрация**: Apache Airflow 2.8.1
- **Контейнеризация**: Docker, Docker Compose
- **Язык программирования**: Python 3.11
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

business_case_rocket_26/
├── dags/
│   └── listing_sabitova_rocket.py
├── app/
│   └── app.py
├── data/
├── logs/
├── ml.ipynb
├── Dockerfile
├── docker-compose.yml

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
from datetime import datetime
import glob
 
st.set_page_config(page_title="Аналитика космических запусков - Сабитова", layout="wide")
 
DATA_DIR = "/opt/airflow/data"
JSON_FILE = f"{DATA_DIR}/launches.json"
PREDICTIONS_FILE = f"{DATA_DIR}/ml_predictions.csv"
IMAGES_DIR = f"{DATA_DIR}/images"
 
st.title("🚀 Аналитика космических запусков")
st.markdown("### Вариант 12: Мониторинг успешных запусков и анализ уязвимостей")
st.markdown("**DAG:** listing_sabitova_rocket")
 
# --- Tabs ---
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Основная аналитика",
    "📸 Отчет по изображениям",
    "📈 Мониторинг запусков DAG",
    "🔒 Анализ уязвимостей"
])
 
# --- TAB 1 ---
with tab1:
    st.header("Ближайшие запуски")
 
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r") as f:
            launches_data = json.load(f)
            launches = launches_data.get("results", [])
 
        if launches:
            df_launches = pd.DataFrame([{
                "Имя миссии": l.get("name"),
                "Статус": l.get("status", {}).get("name") if isinstance(l.get("status"), dict) else l.get("status"),
                "Окно старта": l.get("window_start"),
                "Провайдер": l.get("launch_service_provider", {}).get("name")
                if isinstance(l.get("launch_service_provider"), dict) else None
            } for l in launches])
 
            st.dataframe(df_launches)
 
            col1, col2 = st.columns(2)
 
            with col1:
                st.subheader("Запуски по провайдерам")
                st.bar_chart(df_launches["Провайдер"].value_counts())
 
            with col2:
                st.subheader("Статусы запусков")
                st.bar_chart(df_launches["Статус"].value_counts())
 
        else:
            st.warning("Нет данных")
 
    else:
        st.warning("Запустите DAG")
 
# --- TAB 2 ---
with tab2:
    st.header("📸 Отчет по незагруженным изображениям")
 
    report_path = f"{DATA_DIR}/failed_images_report.json"
 
    if os.path.exists(report_path):
        with open(report_path) as f:
            report = json.load(f)
 
        st.metric("Всего неудачных загрузок", report.get("total_failed", 0))
 
        if report.get("failed_images"):
            df = pd.DataFrame(report["failed_images"])
            st.dataframe(df)
        else:
            st.success("Все изображения загружены")
 
    else:
        st.info("Нет отчета")
 
# --- TAB 3 (ИСПРАВЛЕНО) ---
with tab3:
    st.header("📈 Мониторинг запусков DAG")
 
    monitoring_files = glob.glob(f"{DATA_DIR}/launch_monitoring_*.json")
 
    if monitoring_files:
 
        # --- АГРЕГАЦИЯ ---
        total_success = 0
        total_failed = 0
        history_table = []
 
        for file_path in monitoring_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
 
                    success = int(data.get('successful_launches_count', 0))
                    failed = int(data.get('failed_launches_count', 0))
 
                    total_success += success
                    total_failed += failed
 
                    history_table.append({
                        "Дата запуска": data.get('execution_date', '—')[:19],
                        "Статус": data.get('status', 'UNKNOWN'),
                        "Run ID": str(data.get('run_id', '—'))[:35],
                        "Длительность (сек)": round(float(data.get('duration_seconds', 0)), 1)
                        if data.get('duration_seconds') else "—"
                    })
 
            except:
                continue
 
        # --- МЕТРИКИ ---
        col1, col2, col3 = st.columns(3)
 
        with col1:
            st.metric("✅ Успешные запуски DAG", total_success)
 
        with col2:
            st.metric("❌ Неудачные запуски DAG", total_failed)
 
        with col3:
            total = total_success + total_failed
            success_rate = (total_success / total * 100) if total > 0 else 0
            st.metric("📈 Success Rate", f"{success_rate:.1f}%")
 
        # --- ГРАФИК ---
        st.subheader("📊 Общее соотношение запусков")
 
        df_summary = pd.DataFrame({
            "Тип": ["Успешные", "Неудачные"],
            "Количество": [total_success, total_failed]
        })
 
        st.bar_chart(df_summary.set_index("Тип"))
 
        # --- ИСТОРИЯ ---
        st.subheader("История запусков")
 
        if history_table:
            df_history = pd.DataFrame(history_table)
            st.dataframe(df_history, use_container_width=True, hide_index=True)
        else:
            st.info("История пуста")
 
    else:
        st.info("Запустите DAG несколько раз")
 
# --- TAB 4 ---
with tab4:
    st.header("🔒 Анализ уязвимостей")
 
    vuln_files = glob.glob(f"{DATA_DIR}/vulnerability_analysis_*.json")
 
    if vuln_files:
        latest = max(vuln_files, key=os.path.getctime)
 
        with open(latest) as f:
            report = json.load(f)
 
        # --- Метрика ---
        total_vuln = report.get("total_vulnerabilities", 0)
        st.metric("Всего уязвимостей", total_vuln)
 
        # --- Таблица + график ---
        if report.get("vulnerabilities"):
            df = pd.DataFrame(report["vulnerabilities"])

 
            # ================= ГРАФИК =================
            st.subheader("📊 Распределение уязвимостей по критичности")
 
            severity_counts = {
                'CRITICAL': 0,
                'HIGH': 0,
                'MEDIUM': 0,
                'LOW': 0
            }
 
            for vuln in report['vulnerabilities']:
                severity = vuln.get('severity', 'LOW')
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
 
            severity_data = pd.DataFrame({
                'Критичность': list(severity_counts.keys()),
                'Количество': list(severity_counts.values())
            })
 
            # сортировка (как в твоём втором коде)
            severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
            severity_data['order'] = severity_data['Критичность'].map(severity_order)
            severity_data = severity_data.sort_values('order')
 
            # убираем нулевые значения
            severity_data = severity_data[severity_data['Количество'] > 0]
 
            if not severity_data.empty:
                st.bar_chart(severity_data.set_index('Критичность')['Количество'])
            else:
                st.info("Нет данных для графика")
 
            # ================= (опционально) детали =================
            st.subheader("🔍 Детали уязвимостей")
 
            severity_icons = {
                'CRITICAL': '🔴',
                'HIGH': '🟠',
                'MEDIUM': '🟡',
                'LOW': '🔵'
            }
 
            for vuln in report['vulnerabilities']:
                severity = vuln.get('severity', 'LOW')
                icon = severity_icons.get(severity, '⚪')
 
                line = vuln.get('line', '?')
                message = vuln.get('message', '')
                code = vuln.get('code', '')
 
                with st.expander(f"{icon} [{severity}] Строка {line}: {message[:80]}"):
                    st.code(code, language='python')
                    st.write(message)
 
        else:
            st.success("Уязвимости не найдены 🎉")
 
    else:
        st.info("Нет отчета")
```

## ml.ipynb

Jupyter ноутбук для ML обработки:
- Загружает фотографии из `./data/images/`
- Прогоняет их через нейросеть CLIP (Zero-Shot Classification)
- Сохраняет предсказания в `ml_predictions.csv`
