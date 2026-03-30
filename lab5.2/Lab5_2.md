# Лабораторная работа работа 5.2 Разработка алгоритмов для трансформации данных. Airflow DAG

# Цель работы

1. Закрепить навыки развертывания Apache Airflow в контейнеризированной среде (Docker).
2. Изучить работу с JSON-данными и бинарным контентом (изображениями) внутри ETL-процесса.
3. Научиться проектировать архитектуру ETL-решений и визуализировать её.
4. Автоматизировать выгрузку результатов работы DAG из контейнера в хост-систему.

# Архитектура решения

graph TB
    subgraph Source_Layer["📡 Source Layer (Источники данных)"]
        API[Launch Library 2 API<br/>REST API]
        USER[Пользователь<br/>Веб-браузер]
    end

    subgraph Storage_Layer["💾 Storage Layer (Хранилище данных)"]
        DATA["./data/<br/>- images/<br/>- launches.json<br/>- failed_images_report.json<br/>- launch_monitoring_*.json<br/>- vulnerability_analysis_*.json"]
        LOGS["./logs/<br/>Логи Airflow"]
        DAGS["./dags/<br/>listing_sabitova_rocket.py"]
    end

    subgraph Business_Layer["⚙️ Business Layer (Бизнес-логика)"]
        AIRFLOW[Apache Airflow<br/>ETL Pipeline<br/>Порт: 8080]
        STREAMLIT[Streamlit<br/>BI Dashboard<br/>Порт: 8501]
        JUPYTER[Jupyter Notebook<br/>ML обработка<br/>Порт: 8888]
    end

    subgraph ML_Layer["🧠 ML Layer (ИИ-обработка)"]
        CLIP[CLIP Neural Network<br/>Zero-Shot Classification]
    end

    API -->|HTTP GET /launches/upcoming| AIRFLOW
    AIRFLOW -->|Сохраняет JSON и фото| DATA
    AIRFLOW -->|Пишет логи| LOGS
    AIRFLOW -->|Читает DAG| DAGS
    
    JUPYTER -->|Читает фото| DATA
    JUPYTER -->|CLIP анализ| CLIP
    JUPYTER -->|Сохраняет predictions| DATA
    
    STREAMLIT -->|Читает отчеты| DATA
    STREAMLIT -->|Показывает дашборд| USER
    
    USER -->|Запуск DAG| AIRFLOW
