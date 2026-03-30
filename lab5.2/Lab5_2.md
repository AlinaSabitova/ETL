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

## Внешние источники и Пользователь
- **Launch Library 2 API** — внешний REST API, откуда Airflow получает данные о запусках и ссылки на фото.
- **Пользователь** — взаимодействует с системой через браузер (порты 8080, 8888, 8501).

## Хост-система и локальные папки (Bind Mounts)
На Ubuntu проброшены папки:
- `./dags` — код DAG (`listing_sabitova_rocket.py`)
- `./data` — озеро данных (JSON, фото, отчеты)
- `./logs` — логи Airflow

## Apache Airflow (ETL контур)
- **Scheduler** — запускает DAG, загружает данные из API, скачивает фото.
- **Webserver** — UI для управления и мониторинга.
- **PostgreSQL** — хранит метаданные Airflow.

## Jupyter (ML контур)
- Запускает CLIP нейросеть для классификации фото ракет.
- Сохраняет предсказания в `ml_predictions.csv`.

## Streamlit (Аналитический контур)
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
